import fitz
import os
import re
import json
import uuid
import requests



def create_unique_folder(parent_folder):
    folder_name = str(uuid.uuid4())
    new_folder_path = f"{parent_folder}/{folder_name}"
    os.makedirs(new_folder_path)
    return new_folder_path


def get_estimate_information(pdf_document, language):
    wOwner = "Owner"
    wIns = "Insurance Company"
    wVin = "VIN"
    wOdo = "Odometer"
    wParts = "Parts Profile"
    wFooter = "Mitchell Cloud Estimating"
    if language == "fr":
        wOwner = "Propriétaire"
        wIns = "Assureur"
        wVin = "NIV"
        wOdo = "Odomètre"
        wParts = "Profil de pièces"
    totalDocumentPage = pdf_document.page_count

    car_owner = car_ins = car_vin = car_odo = car_name = "N/A"
    last_text = ""
    for page_num in range(totalDocumentPage):
        if page_num == 0 or page_num == 1:
            page = pdf_document.load_page(page_num)
            text = page.get_text("text")
            # Remove content from "Mitchell Cloud Estimating" to the bottom
            if wFooter in text:
                if page.search_for(wFooter):
                    footer_pos = page.search_for(wFooter)[0]
                    rect_to_remove = fitz.Rect(
                        0, footer_pos.y1 - 20, page.rect.width, page.rect.height
                    )
                    page.add_redact_annot(
                        rect_to_remove, fill=(1, 1, 1)
                    )  # White out the area
                    page.apply_redactions()

            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        if last_text == wIns and car_ins == "N/A":
                            ins_block_text = ""
                            for bLine in block["lines"]:
                                if bLine["spans"][0]["text"] != wIns:
                                    if ins_block_text != "":
                                        ins_block_text += (
                                            " " + bLine["spans"][0]["text"]
                                        )
                                    else:
                                        ins_block_text += bLine["spans"][0]["text"]
                            car_ins = ins_block_text

                        for span in line["spans"]:
                            if last_text == wOwner:
                                car_owner = span["text"]
                            if last_text == wIns and car_ins == "N/A":
                                car_ins = span["text"]
                            if last_text == wVin:
                                car_vin = span["text"]
                            if last_text == wOdo:
                                car_odo = span["text"]
                            if span["text"] == wParts:
                                car_name = last_text
                            last_text = span["text"]

    car_data = {
        "car_owner": car_owner,
        "car_ins": car_ins,
        "car_vin": car_vin,
        "car_odo": car_odo,
        "car_name": car_name,
    }

    return car_data


def get_estimate_information_audatex(pdf_document, language):
    wOwner = "Owner:"
    wIns = "Ins. Company:"
    wVin = "VIN:"
    wOdo = "Kilometer:"
    wCarName = "Vehicle"
    if language == "fr":
        wOwner = "Propriétaire:"
        wIns = "Compagnie:"
        wVin = "NIV:"
        wOdo = "Odomètre:"
        wCarName = "Véhicule"

    totalDocumentPage = pdf_document.page_count

    car_owner = car_ins = car_vin = car_odo = car_name = "N/A"
    last_text = ""
    for page_num in range(totalDocumentPage):
        if page_num == 0 or page_num == 1:
            page = pdf_document.load_page(page_num)
            text = page.get_text("text")

            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        for span in line["spans"]:
                            if last_text == wOwner and car_owner == "N/A":
                                car_owner = span["text"]
                            if last_text == wIns and car_ins == "N/A":
                                car_ins = span["text"]
                            if last_text == wVin and car_vin == "N/A":
                                car_vin = span["text"]
                            if last_text == wOdo and car_odo == "N/A":
                                car_odo = span["text"]
                            if last_text.strip() == wCarName and car_name == "N/A":
                                car_name = span["text"]
                            last_text = span["text"]

    car_data = {
        "car_owner": car_owner.strip(),
        "car_ins": car_ins.strip(),
        "car_vin": car_vin.strip(),
        "car_odo": car_odo.strip(),
        "car_name": car_name.strip(),
    }

    return car_data


def clean_pdf_audatex(pdf_document, parent_path, language="en"):
    merge_pdf_to_single_page_path = merge_pdf_to_single_page(pdf_document, parent_path)
    pdf_document = fitz.open(merge_pdf_to_single_page_path)

    wLine = "Line"
    wManufact = "MFR.Part No."
    wItems = "Items"
    wEndTable = "Estimate Total & Entries"
    wDamagesCombined = "Combined Previous Damages"

    if language == "fr":
        wLine = "Ligne"
        wManufact = "# Pièce Manufact."
        wItems = "Items"
        wEndTable = "Calcul final & Entrées"
        wDamagesCombined = "Dommages antérieurs combines"

    new_pdf = fitz.open()
    totalDocumentPage = pdf_document.page_count
    table_end = False
    table_start = False
    for page_num in range(totalDocumentPage):
        if table_end == False:
            page = pdf_document.load_page(page_num)
            text = page.get_text("text")

            # Remove content from 20px from bottom to the bottom
            rect_to_remove = fitz.Rect(
                0, page.rect.height - 52, page.rect.width, page.rect.height
            )
            page.add_redact_annot(rect_to_remove, fill=(1, 1, 1))
            page.apply_redactions()

            if page_num != 0:
                # Remove header, 0 to 20px from top
                rect_to_remove = fitz.Rect(0, 0, page.rect.width, 52)
                page.add_redact_annot(rect_to_remove, fill=(1, 1, 1))
                page.apply_redactions()

            if wManufact in text and not table_start:
                table_start = True
                labor_pos = page.search_for(wManufact)[0]
                rect_to_remove = fitz.Rect(0, 0, page.rect.width, labor_pos.y0)
                page.add_redact_annot(
                    rect_to_remove, fill=(1, 1, 1)
                )  # White out the area
                page.apply_redactions()

            if not table_start:
                page.add_redact_annot(page.rect, fill=(1, 1, 1))
                page.apply_redactions()

            if wItems in text:
                if wDamagesCombined in text:
                    dmg_text = page.search_for(wDamagesCombined)[0]
                    items_text = page.search_for(wItems)[0]
                    if dmg_text.y0 > items_text.y0:
                        # Remove content from "Items" to "Dommages antérieurs combines"
                        rect_to_remove = fitz.Rect(
                            0, items_text.y0 - 1, page.rect.width, dmg_text.y1
                        )
                        page.add_redact_annot(rect_to_remove, fill=(1, 1, 1))
                        page.apply_redactions()
                if wEndTable in text:
                    calc_text = page.search_for(wEndTable)[0]
                    items_text = page.search_for(wItems)[0]
                    if calc_text.y0 > items_text.y0:
                        # Remove content from "Items" to bottom
                        rect_to_remove = fitz.Rect(
                            0, items_text.y1 - 2, page.rect.width, page.rect.height
                        )
                        page.add_redact_annot(rect_to_remove, fill=(1, 1, 1))
                        page.apply_redactions()
                        table_end = True

                if wDamagesCombined not in text and wEndTable not in text:
                    items_text = page.search_for(wItems)[0]
                    # Remove content from "Items" to the bottom
                    rect_to_remove = fitz.Rect(
                        0, items_text.y0 - 2, page.rect.width, page.rect.height
                    )
                    page.add_redact_annot(rect_to_remove, fill=(1, 1, 1))
                    page.apply_redactions()

            text_after_modified = page.get_text("text")
            if text_after_modified:
                new_page = new_pdf.new_page(
                    width=page.rect.width, height=page.rect.height
                )
                new_page.show_pdf_page(new_page.rect, pdf_document, page_num)

    output_pdf_path = f"{parent_path}/pdf_table_data.pdf"
    new_pdf.save(output_pdf_path)
    new_pdf.close()
    return output_pdf_path


def is_duplicated_text(text):
    if text:
        test_duplicated = text.replace("\n", "")
        pattern = r"(.*?)\1+$"
        match = re.match(pattern, test_duplicated)
        return match is not None

    return False


def remove_duplicated_text(text):
    if text:
        # This regex removes consecutive duplicate sequences
        pattern = r"(.*?)\1+$"
        test_duplicated = text.replace("\n", "")
        cleaned_text = re.sub(pattern, r"\1", test_duplicated)
        return cleaned_text

    return text


def merge_pdf_to_single_page(pdf_document, parent_path):
    # Open the input PDF
    # pdf_document = fitz.open(input_pdf_path)

    # Create a new PDF for the output
    output_pdf = fitz.open()

    # Get the width and height of the first page to set the new page size
    first_page = pdf_document[0]
    page_width = first_page.rect.width
    total_height = 0

    # Calculate the total height of all pages
    for page in pdf_document:
        total_height += page.rect.height

    # Create a new page with the total height and the same width
    new_page = output_pdf.new_page(width=page_width, height=total_height)

    # Position for placing the pages on the new page
    current_height = 0

    # Iterate through each page and insert it into the new page
    for page in pdf_document:
        new_page.show_pdf_page(
            fitz.Rect(0, current_height, page_width, current_height + page.rect.height),
            pdf_document,
            page.number,
        )
        current_height += page.rect.height

    # Save the output PDF
    merge_pdf_to_single_page_path = f"{parent_path}/pdf_merged.pdf"
    output_pdf.save(merge_pdf_to_single_page_path)
    output_pdf.close()
    # pdf_document.close()
    return merge_pdf_to_single_page_path


def clean_pdf_mitchell(pdf_document, parent_path, language="en"):
    wLine = "Line #"
    wFooter = "Mitchell Cloud Estimating"
    wEndTable = "* Judgment Item"

    if language == "fr":
        wLine = "Ligne #"
        wFooter = "Mitchell Cloud Estimating"
        wEndTable = "* Point de jugement"

    new_pdf = fitz.open()
    hasTitle = False
    table_end = False
    totalDocumentPage = pdf_document.page_count

    for page_num in range(totalDocumentPage):
        if table_end == False:
            page = pdf_document.load_page(page_num)
            text = page.get_text("text")
            isTablePage = False

            # Remove content from the top to "LABOR PART"
            if wLine in text:
                labor_pos = page.search_for(wLine)[0]
                if hasTitle:
                    posY = labor_pos.y0 + 10
                    rect_to_remove = fitz.Rect(0, 0, page.rect.width, posY)
                else:
                    rect_to_remove = fitz.Rect(0, 0, page.rect.width, labor_pos.y0)
                page.add_redact_annot(
                    rect_to_remove, fill=(1, 1, 1)
                )  # White out the area
                page.apply_redactions()
                isTablePage = True
                hasTitle = True

            # Remove content from "* Judgment Item" to the bottom
            if wEndTable in text:
                if page.search_for(wEndTable):
                    judgment_pos = page.search_for(wEndTable)[0]
                else:
                    judgment_pos = find_text_pos(wEndTable, page)

                rect_to_remove = fitz.Rect(
                    0, judgment_pos.y1 - 10, page.rect.width, page.rect.height
                )
                page.add_redact_annot(
                    rect_to_remove, fill=(1, 1, 1)
                )  # White out the area
                page.apply_redactions()
                table_end = True

            # Remove content from "Mitchell Cloud Estimating" to the bottom
            if wFooter in text:
                if page.search_for(wFooter):
                    footer_pos = page.search_for(wFooter)[0]
                    rect_to_remove = fitz.Rect(
                        0, footer_pos.y1 - 20, page.rect.width, page.rect.height
                    )
                    page.add_redact_annot(
                        rect_to_remove, fill=(1, 1, 1)
                    )  # White out the area
                    page.apply_redactions()

            # Remove content of the page if it is not a table page
            if not isTablePage:
                page.add_redact_annot(page.rect, fill=(1, 1, 1))
                page.apply_redactions()

            # Insert the modified page into the new PDF
            text_after_modified = page.get_text("text")
            if text_after_modified:
                new_page = new_pdf.new_page(
                    width=page.rect.width, height=page.rect.height
                )
                new_page.show_pdf_page(new_page.rect, pdf_document, page_num)

    output_pdf_path = f"{parent_path}/pdf_table_data.pdf"
    new_pdf.save(output_pdf_path)
    new_pdf.close()
    return output_pdf_path


def read_text_by_pos(page, bbox_x0, bbox_y0, bbox_x1, bbox_y1):
    bbox = (bbox_x0, bbox_y0, bbox_x1, bbox_y1)
    rect = fitz.Rect(bbox)
    text = page.get_text("text", clip=rect)
    return text


def read_text_by_pos_mc(page, bbox_x0, bbox_y0, bbox_x1, bbox_y1):
    bbox = (bbox_x0, bbox_y0, bbox_x1, bbox_y1)
    rect = fitz.Rect(bbox)
    text = page.get_text("text", clip=rect)
    text = text.replace("\n", " ").strip()
    return text


def get_next_part_pos(part, line_part_start_pos):
    next_part_bbox_y0 = 0
    curren_index = 999
    for index, lData in enumerate(line_part_start_pos):
        if part == lData["part"]:
            curren_index = index
            break

    if curren_index != 999:
        next_part_index = curren_index + 1
        if next_part_index < len(line_part_start_pos):
            next_part_bbox_y0 = line_part_start_pos[next_part_index]["bbox"][1]

    return next_part_bbox_y0


def read_text_mitchell_type_1(pdf, language):
    # En words
    wLine = "Line #"
    wDes = "Description"
    wOpe = "Operation"
    wType = "Type"
    wTotU = "Total Units"
    wNum = "Number"
    wQty = "Qty"
    wTotP = "Total Price"
    wTax = "Tax"
    # Fr words
    if language == "fr":
        wLine = "Ligne #"
        wDes = "Description"
        wOpe = "Opération"
        wType = "Type"
        wTotU = "Unités totales"
        wNum = "Numéro"
        wQty = "Qté"
        wTotP = "Prix total"
        wTax = "Taxe"

    pdf_document = fitz.open(pdf)
    totalDocumentPage = pdf_document.page_count
    xLine = xDes = xOpe = xType1 = xTotU = xType2 = xNum = xQty = xTotP = xTax = 0
    lines = []
    lPart = ""
    for page_num in range(totalDocumentPage):
        line_start_pos = []
        line_part_start_pos = []
        page = pdf_document.load_page(page_num)
        text = page.get_text("text")
        if wLine in text:
            xLine = page.search_for(wLine)[0].x0
            xDes = page.search_for(wDes)[0].x0
            xOpe = page.search_for(wOpe)[0].x0
            xType1 = page.search_for(wType)[0].x0
            xTotU = page.search_for(wTotU)[0].x0
            xType2 = page.search_for(wType)[1].x0
            xNum = page.search_for(wNum)[0].x0
            xQty = page.search_for(wQty)[0].x0
            xTotP = page.search_for(wTotP)[0].x0
            xTax = page.search_for(wTax)[0].x0

        current_y = None
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if "lines" in block:
                for line in block["lines"]:
                    for span in line["spans"]:
                        if span["bbox"][0] == xLine:
                            lPart = span["text"]
                            line_part_start_pos.append(
                                {"part": lPart, "bbox": span["bbox"]}
                            )

                            line_start_pos.append(
                                {"type": "part", "part": lPart, "bbox": span["bbox"]}
                            )
                        if span["bbox"][0] < xDes and span["bbox"][0] != xLine:
                            line_start_pos.append(
                                {"type": "line", "part": lPart, "bbox": span["bbox"]}
                            )
        for index, lData in enumerate(line_start_pos):
            if lData["type"] == "line":
                nIndex = index + 1
                nPart_bbox_y0 = get_next_part_pos(lData["part"], line_part_start_pos)
                if nIndex < len(line_start_pos):
                    nBbox_y0 = line_start_pos[nIndex]["bbox"][1]
                    if nPart_bbox_y0 != 0 and nPart_bbox_y0 < nBbox_y0:
                        nBbox_y0 = nPart_bbox_y0
                else:
                    nBbox_y0 = page.rect.height
                lineDbr = read_text_by_pos_mc(
                    page, xLine + 25, lData["bbox"][1], xDes - 3, nBbox_y0 - 3
                )
                lineDes = read_text_by_pos_mc(
                    page, xDes, lData["bbox"][1], xOpe - 3, nBbox_y0 - 3
                )
                lineOpe = read_text_by_pos_mc(
                    page, xOpe, lData["bbox"][1], xType1 - 3, nBbox_y0 - 3
                )
                lineType = read_text_by_pos_mc(
                    page, xType1, lData["bbox"][1], xTotU - 3, nBbox_y0 - 3
                )
                lineTotU = read_text_by_pos_mc(
                    page, xTotU, lData["bbox"][1], xType2 - 3, nBbox_y0 - 3
                )
                lineType2 = read_text_by_pos_mc(
                    page, xType2, lData["bbox"][1], xNum - 3, nBbox_y0 - 3
                )
                lineNum = read_text_by_pos_mc(
                    page, xNum, lData["bbox"][1], xQty - 3, nBbox_y0 - 3
                )
                lineQty = read_text_by_pos_mc(
                    page, xQty, lData["bbox"][1], xTotP - 3, nBbox_y0 - 3
                )
                lineTotP = read_text_by_pos_mc(
                    page, xTotP, lData["bbox"][1], xTax - 3, nBbox_y0 - 3
                )
                lineTax = read_text_by_pos_mc(
                    page, xTax, lData["bbox"][1], xTax + 40, nBbox_y0 - 3
                )
                if any(
                    [
                        lineDbr,
                        lineDes,
                        lineOpe,
                        lineType,
                        lineTotU,
                        lineType2,
                        lineNum,
                        lineQty,
                        lineTotP,
                        lineTax,
                    ]
                ):
                    lines.append(
                        {
                            "header": lData["part"],
                            "dbRef": lineDbr if lineDbr else "N/A",
                            "description": lineDes if lineDes else "N/A",
                            "operation": lineOpe if lineOpe else "N/A",
                            "Type": lineType if lineType else "N/A",
                            "TotalUnits": lineTotU if lineTotU else "N/A",
                            "Type2": lineType2 if lineType2 else "N/A",
                            "Number": lineNum if lineNum else "N/A",
                            "Qty": lineQty if lineQty else "N/A",
                            "TotalPrice": lineTotP if lineTotP else "N/A",
                            "Tax": lineTax if lineTax else "N/A",
                        }
                    )
    return lines


def read_text_mitchell_type_2(pdf, language):
    # En words
    wLine = "Line #"
    wDes = "Description"
    wOpe = "Operation"
    wType = "Type"
    wTotU = "Total Units"
    wNum = "Number"
    wQty = "Qty"
    wTotP = "Total Price"
    wTax = "Tax"
    wCEG = "CEG"
    # Fr words
    if language == "fr":
        wLine = "Ligne #"
        wDes = "Description"
        wOpe = "Opération"
        wType = "Type"
        wTotU = "Unités totales"
        wNum = "Numéro"
        wQty = "Qté"
        wTotP = "Prix total"
        wTax = "Taxe"
        wCEG = "CEG"

    pdf_document = fitz.open(pdf)
    totalDocumentPage = pdf_document.page_count
    xLine = xDes = xOpe = xType1 = xTotU = xType2 = xNum = xQty = xTotP = xTax = 0
    lines = []
    lPart = ""
    for page_num in range(totalDocumentPage):
        line_start_pos = []
        line_part_start_pos = []
        page = pdf_document.load_page(page_num)
        text = page.get_text("text")
        if wLine in text:
            xLine = page.search_for(wLine)[0].x0
            xDes = page.search_for(wDes)[0].x0
            xOpe = page.search_for(wOpe)[0].x0
            xType1 = page.search_for(wType)[0].x0
            xTotU = page.search_for(wTotU)[0].x0
            xCEG = page.search_for(wCEG)[0].x0
            xType2 = page.search_for(wType)[1].x0
            xNum = page.search_for(wNum)[0].x0
            xQty = page.search_for(wQty)[0].x0
            xTotP = page.search_for(wTotP)[0].x0
            xTax = page.search_for(wTax)[0].x0

        current_y = None
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if "lines" in block:
                for line in block["lines"]:
                    for span in line["spans"]:
                        if span["bbox"][0] == xLine:
                            lPart = span["text"]
                            line_part_start_pos.append(
                                {"part": lPart, "bbox": span["bbox"]}
                            )

                            line_start_pos.append(
                                {"type": "part", "part": lPart, "bbox": span["bbox"]}
                            )
                        if span["bbox"][0] < xDes and span["bbox"][0] != xLine:
                            line_start_pos.append(
                                {"type": "line", "part": lPart, "bbox": span["bbox"]}
                            )

        for index, lData in enumerate(line_start_pos):
            if lData["type"] == "line":
                nIndex = index + 1
                nPart_bbox_y0 = get_next_part_pos(lData["part"], line_part_start_pos)
                if nIndex < len(line_start_pos):
                    nBbox_y0 = line_start_pos[nIndex]["bbox"][1]
                    if nPart_bbox_y0 != 0 and nPart_bbox_y0 < nBbox_y0:
                        nBbox_y0 = nPart_bbox_y0
                else:
                    nBbox_y0 = page.rect.height
                lineDbr = read_text_by_pos_mc(
                    page, xLine + 25, lData["bbox"][1], xDes - 3, nBbox_y0 - 3
                )
                lineDes = read_text_by_pos_mc(
                    page, xDes, lData["bbox"][1], xOpe - 3, nBbox_y0 - 3
                )
                lineOpe = read_text_by_pos_mc(
                    page, xOpe, lData["bbox"][1], xType1 - 3, nBbox_y0 - 3
                )
                lineType = read_text_by_pos_mc(
                    page, xType1, lData["bbox"][1], xTotU - 3, nBbox_y0 - 3
                )
                lineTotU = read_text_by_pos_mc(
                    page, xTotU, lData["bbox"][1], xCEG - 3, nBbox_y0 - 3
                )
                lineCEG = read_text_by_pos_mc(
                    page, xCEG, lData["bbox"][1], xType2 - 3, nBbox_y0 - 3
                )
                lineType2 = read_text_by_pos_mc(
                    page, xType2, lData["bbox"][1], xNum - 3, nBbox_y0 - 3
                )
                lineNum = read_text_by_pos_mc(
                    page, xNum, lData["bbox"][1], xQty - 3, nBbox_y0 - 3
                )
                lineQty = read_text_by_pos_mc(
                    page, xQty, lData["bbox"][1], xTotP - 3, nBbox_y0 - 3
                )
                lineTotP = read_text_by_pos_mc(
                    page, xTotP, lData["bbox"][1], xTax - 3, nBbox_y0 - 3
                )
                lineTax = read_text_by_pos_mc(
                    page, xTax, lData["bbox"][1], xTax + 40, nBbox_y0 - 3
                )
                if any(
                    [
                        lineDbr,
                        lineDes,
                        lineOpe,
                        lineType,
                        lineTotU,
                        lineCEG,
                        lineType2,
                        lineNum,
                        lineQty,
                        lineTotP,
                        lineTax,
                    ]
                ):
                    lines.append(
                        {
                            "header": lData["part"],
                            "dbRef": lineDbr if lineDbr else "N/A",
                            "description": lineDes if lineDes else "N/A",
                            "operation": lineOpe if lineOpe else "N/A",
                            "Type": lineType if lineType else "N/A",
                            "TotalUnits": lineTotU if lineTotU else "N/A",
                            "CEG": lineCEG if lineCEG else "N/A",
                            "Type2": lineType2 if lineType2 else "N/A",
                            "Number": lineNum if lineNum else "N/A",
                            "Qty": lineQty if lineQty else "N/A",
                            "TotalPrice": lineTotP if lineTotP else "N/A",
                            "Tax": lineTax if lineTax else "N/A",
                        }
                    )

    return lines


def read_text_audatex(pdf, language="en"):
    # En words
    wLine = "Line"
    wOp = "Op"
    wGuide = "Guide"
    wMC = "MC"
    wDescription = "Description"
    wManufact = "MFR.Part No."
    wPrix = "Price"
    wAjust = "ADJ%"
    wR = "B%"
    wHeures = "Hours"
    wT = "R"
    # Fr words
    if language == "fr":
        wLine = "Ligne"
        wOp = "Op"
        wGuide = "Guide"
        wMC = "MC"
        wDescription = "Description"
        wManufact = "# Pièce Manufact."
        wPrix = "Prix"
        wAjust = "Ajust%"
        wR = "R%"
        wHeures = "Heures"
        wT = "T"

    pdf_document = fitz.open(pdf)
    xLine = xOp = xGuide = xMC = xDescription = xManufact = xPrix = xAjust = xR = (
        xHeures
    ) = xT = None
    xPart = 24
    lines = []
    second_lines = []
    lPart = ""

    pos_finnish_man = 0
    line_start_pos = []
    line_part_start_pos = []
    page = pdf_document.load_page(0)
    page_width = page.rect.width
    text = page.get_text("text")
    if wLine in text:
        xLine = page.search_for(wLine)[0]
        xOp = page.search_for(wOp)[0]
        xGuide = page.search_for(wGuide)[0]
        xMC = page.search_for(wMC)[0]
        xDescription = page.search_for(wDescription)[0]
        xManufact = page.search_for(wManufact)[0]
        xPrix = page.search_for(wPrix)[0]
        xAjust = page.search_for(wAjust)[0]
        xR = page.search_for(wR)[0]
        xHeures = page.search_for(wHeures)[0]
        xT = page.search_for(wT)[0]

    pdf_ended_at = page.rect.height
    table_headers = page.search_for(wLine)
    table_headers_2 = page.search_for(wManufact)
    if len(table_headers) > 1 and len(table_headers_2) > 1:
        pdf_ended_at = table_headers[1].y0
        second_lines = read_text_audatex_second_table(pdf, language)

    last_line_pos = 0
    current_y = None
    blocks = page.get_text("dict")["blocks"]
    for block in blocks:
        if "lines" in block:
            for line in block["lines"]:
                for span in line["spans"]:
                    if span["bbox"][1] < pdf_ended_at:
                        if span["bbox"][0] <= xPart:
                            lPart = span["text"]
                            line_part_start_pos.append(
                                {"part": lPart, "bbox": span["bbox"]}
                            )

                            line_start_pos.append(
                                {
                                    "type": "part",
                                    "part": lPart,
                                    "bbox": span["bbox"],
                                    "sub_lines": "",
                                    "next_line_y0": "",
                                }
                            )

                        if span["bbox"][0] <= xLine.x1 and span["bbox"][0] > xLine.x0:
                            if (
                                len(line_start_pos)
                                and line_start_pos[-1]["next_line_y0"] == ""
                            ):
                                line_start_pos[-1]["next_line_y0"] = span["bbox"][1]

                            line_start_pos.append(
                                {
                                    "type": "line",
                                    "part": lPart,
                                    "bbox": span["bbox"],
                                    "sub_lines": "",
                                    "next_line_y0": "",
                                }
                            )

                        if abs(span["bbox"][3] - last_line_pos) > 2:
                            # This is sub line of description
                            if abs(span["bbox"][0] - xDescription.x0) < 1 and len(
                                line_start_pos
                            ):
                                # Update text to the last item line_start_pos
                                sub_line = span["text"]
                                line_start_pos[-1]["sub_lines"] += f"\n{sub_line}"
                                if line_start_pos[-1]["next_line_y0"] == "":
                                    line_start_pos[-1]["next_line_y0"] = span["bbox"][1]

                            last_line_pos = span["bbox"][3]

                        if abs(span["bbox"][0] - xManufact.x0) < 1:
                            if pos_finnish_man < span["bbox"][2]:
                                pos_finnish_man = span["bbox"][2]

    for index, lData in enumerate(line_start_pos):
        if lData["type"] == "line":
            nIndex = index + 1
            nPart_bbox_y0 = get_next_part_pos(lData["part"], line_part_start_pos)
            if nIndex < len(line_start_pos):
                nBbox_y0 = line_start_pos[nIndex]["bbox"][1]

                if nPart_bbox_y0 != 0 and nPart_bbox_y0 < nBbox_y0:
                    nBbox_y0 = nPart_bbox_y0
            else:
                nBbox_y0 = pdf_ended_at

            if lData["next_line_y0"]:
                nBbox_y0 = lData["next_line_y0"]

            lineOp = read_text_by_pos(
                page, xOp.x0, lData["bbox"][1] + 2, xGuide.x0, nBbox_y0 - 1
            )
            lineGuide = read_text_by_pos(
                page, xOp.x1 + 3, lData["bbox"][1] + 2, xMC.x0 - 3, nBbox_y0 - 1
            )
            lineMC = read_text_by_pos(
                page,
                xMC.x0 - 2,
                lData["bbox"][1] + 2,
                xDescription.x0 - 2,
                nBbox_y0 - 1,
            )
            lineDes = read_text_by_pos(
                page,
                xDescription.x0,
                lData["bbox"][1] + 2,
                xManufact.x0 - 3,
                nBbox_y0 - 1,
            )
            lineMan = read_text_by_pos(
                page, xManufact.x0, lData["bbox"][1] + 2, pos_finnish_man, nBbox_y0 - 1
            )
            linePrix = read_text_by_pos(
                page, pos_finnish_man, lData["bbox"][1] + 2, xPrix.x1 + 3, nBbox_y0 - 1
            )
            lineAuj = read_text_by_pos(
                page, xAjust.x0, lData["bbox"][1] + 2, xAjust.x1, nBbox_y0 - 1
            )
            lineR = read_text_by_pos(
                page, xAjust.x1, lData["bbox"][1] + 2, xHeures.x0 - 3, nBbox_y0 - 1
            )
            lineHeures = read_text_by_pos(
                page, xHeures.x0, lData["bbox"][1] + 2, xHeures.x1 + 3, nBbox_y0 - 1
            )
            lineT = read_text_by_pos(
                page, xHeures.x1 + 5, lData["bbox"][1] + 2, page_width, nBbox_y0 - 1
            )

            if is_duplicated_text(lineDes):
                lineOp = remove_duplicated_text(lineOp)
                lineGuide = remove_duplicated_text(lineGuide)
                lineMC = remove_duplicated_text(lineMC)
                lineDes = remove_duplicated_text(lineDes)
                lineMan = remove_duplicated_text(lineMan)
                linePrix = remove_duplicated_text(linePrix)
                lineAuj = remove_duplicated_text(lineAuj)
                lineR = remove_duplicated_text(lineR)
                lineHeures = remove_duplicated_text(lineHeures)
                lineT = remove_duplicated_text(lineT)
            else:
                lineOp = lineOp.replace("\n", "").strip()
                lineGuide = lineGuide.replace("\n", "").strip()
                lineMC = lineMC.replace("\n", "").strip()
                lineDes = lineDes.replace("\n", "").strip()
                lineMan = lineMan.replace("\n", "").strip()
                linePrix = linePrix.replace("\n", "").strip()
                lineAuj = lineAuj.replace("\n", "").strip()
                lineR = lineR.replace("\n", "").strip()
                lineHeures = lineHeures.replace("\n", "").strip()
                lineT = lineT.replace("\n", "").strip()

            if lData["sub_lines"]:
                sub_lines = lData["sub_lines"]
                lineDes += f"\n{sub_lines}"

            if any(
                [
                    lineOp,
                    lineGuide,
                    lineMC,
                    lineDes,
                    lineMan,
                    linePrix,
                    lineAuj,
                    lineR,
                    lineHeures,
                    lineT,
                ]
            ):
                lines.append(
                    {
                        "header": lData["part"],
                        "operation": lineOp if lineOp else "N/A",
                        "dbRef": lineGuide if lineGuide else "N/A",
                        "lineMC": lineMC if lineMC else "N/A",
                        "description": lineDes if lineDes else "N/A",
                        "Number": lineMan if lineMan else "N/A",
                        "TotalPrice": linePrix if linePrix else "N/A",
                        "lineAuj": lineAuj if lineAuj else "N/A",
                        "lineR": lineR if lineR else "N/A",
                        "TotalUnits": lineHeures if lineHeures else "N/A",
                        "Type": lineT if lineT else "N/A",
                    }
                )

    # merge second lines if not empty
    if len(second_lines):
        lines.extend(second_lines)

    return lines


def read_text_audatex_second_table(pdf, language="en"):
    # En words
    wLine = "Line"
    wOp = "Op"
    wGuide = "Guide"
    wMC = "MC"
    wDescription = "Description"
    wManufact = "MFR.Part No."
    wPrix = "Price"
    wAjust = "ADJ%"
    wR = "B%"
    wHeures = "Hours"
    wT = "R"
    # Fr words
    if language == "fr":
        wLine = "Ligne"
        wOp = "Op"
        wGuide = "Guide"
        wMC = "MC"
        wDescription = "Description"
        wManufact = "# Pièce Manufact."
        wPrix = "Prix"
        wAjust = "Ajust%"
        wR = "R%"
        wHeures = "Heures"
        wT = "T"

    pdf_document = fitz.open(pdf)
    xLine = xOp = xGuide = xMC = xDescription = xManufact = xPrix = xAjust = xR = (
        xHeures
    ) = xT = None
    xPart = 24
    lines = []
    lPart = ""

    pos_finnish_man = 0
    line_start_pos = []
    line_part_start_pos = []
    page = pdf_document.load_page(0)
    page_width = page.rect.width
    text = page.get_text("text")

    if wLine in text:
        xLine = page.search_for(wLine)[1]
        xOp = page.search_for(wOp)[1]
        xGuide = page.search_for(wGuide)[1]
        xMC = page.search_for(wMC)[0]
        xDescription = page.search_for(wDescription)[1]
        xManufact = page.search_for(wManufact)[1]
        xPrix = page.search_for(wPrix)[1]
        xAjust = page.search_for(wAjust)[1]
        xR = page.search_for(wR)[1]
        xHeures = page.search_for(wHeures)[1]
        xT = page.search_for(wT)[1]

    pdf_started_at = page.search_for(wLine)[1].y0

    last_line_pos = 0
    current_y = None
    blocks = page.get_text("dict")["blocks"]
    for block in blocks:
        if "lines" in block:
            for line in block["lines"]:
                for span in line["spans"]:
                    if span["bbox"][1] >= pdf_started_at:
                        if span["bbox"][0] <= xPart:
                            lPart = span["text"]
                            line_part_start_pos.append(
                                {"part": lPart, "bbox": span["bbox"]}
                            )

                            line_start_pos.append(
                                {
                                    "type": "part",
                                    "part": lPart,
                                    "bbox": span["bbox"],
                                    "sub_lines": "",
                                    "next_line_y0": "",
                                }
                            )

                        if span["bbox"][0] <= xLine.x1 and span["bbox"][0] > xLine.x0:
                            if (
                                len(line_start_pos)
                                and line_start_pos[-1]["next_line_y0"] == ""
                            ):
                                line_start_pos[-1]["next_line_y0"] = span["bbox"][1]

                            line_start_pos.append(
                                {
                                    "type": "line",
                                    "part": lPart,
                                    "bbox": span["bbox"],
                                    "sub_lines": "",
                                    "next_line_y0": "",
                                }
                            )

                        if abs(span["bbox"][3] - last_line_pos) > 2:
                            # This is sub line of description
                            if abs(span["bbox"][0] - xDescription.x0) < 1 and len(
                                line_start_pos
                            ):
                                # Update text to the last item line_start_pos
                                sub_line = span["text"]
                                line_start_pos[-1]["sub_lines"] += f"\n{sub_line}"
                                if line_start_pos[-1]["next_line_y0"] == "":
                                    line_start_pos[-1]["next_line_y0"] = span["bbox"][1]

                            last_line_pos = span["bbox"][3]

                        if abs(span["bbox"][0] - xManufact.x0) < 1:
                            if pos_finnish_man < span["bbox"][2]:
                                pos_finnish_man = span["bbox"][2]

    for index, lData in enumerate(line_start_pos):
        if lData["type"] == "line":
            nIndex = index + 1
            nPart_bbox_y0 = get_next_part_pos(lData["part"], line_part_start_pos)
            if nIndex < len(line_start_pos):
                nBbox_y0 = line_start_pos[nIndex]["bbox"][1]

                if nPart_bbox_y0 != 0 and nPart_bbox_y0 < nBbox_y0:
                    nBbox_y0 = nPart_bbox_y0
            else:
                nBbox_y0 = page.rect.height

            if lData["next_line_y0"]:
                nBbox_y0 = lData["next_line_y0"]

            lineOp = read_text_by_pos(
                page, xOp.x0, lData["bbox"][1] + 2, xGuide.x0, nBbox_y0 - 1
            )
            lineGuide = read_text_by_pos(
                page, xOp.x1 + 3, lData["bbox"][1] + 2, xMC.x0 - 3, nBbox_y0 - 1
            )
            lineMC = read_text_by_pos(
                page,
                xMC.x0 - 2,
                lData["bbox"][1] + 2,
                xDescription.x0 - 2,
                nBbox_y0 - 1,
            )
            lineDes = read_text_by_pos(
                page,
                xDescription.x0,
                lData["bbox"][1] + 2,
                xManufact.x0 - 3,
                nBbox_y0 - 1,
            )
            lineMan = read_text_by_pos(
                page, xManufact.x0, lData["bbox"][1] + 2, pos_finnish_man, nBbox_y0 - 1
            )
            linePrix = read_text_by_pos(
                page, pos_finnish_man, lData["bbox"][1] + 2, xPrix.x1 + 3, nBbox_y0 - 1
            )
            lineAuj = read_text_by_pos(
                page, xAjust.x0, lData["bbox"][1] + 2, xAjust.x1, nBbox_y0 - 1
            )
            lineR = read_text_by_pos(
                page, xAjust.x1, lData["bbox"][1] + 2, xHeures.x0 - 3, nBbox_y0 - 1
            )
            lineHeures = read_text_by_pos(
                page, xHeures.x0, lData["bbox"][1] + 2, xHeures.x1 + 3, nBbox_y0 - 1
            )
            lineT = read_text_by_pos(
                page, xHeures.x1 + 5, lData["bbox"][1] + 2, page_width, nBbox_y0 - 1
            )

            if is_duplicated_text(lineDes):
                lineOp = remove_duplicated_text(lineOp)
                lineGuide = remove_duplicated_text(lineGuide)
                lineMC = remove_duplicated_text(lineMC)
                lineDes = remove_duplicated_text(lineDes)
                lineMan = remove_duplicated_text(lineMan)
                linePrix = remove_duplicated_text(linePrix)
                lineAuj = remove_duplicated_text(lineAuj)
                lineR = remove_duplicated_text(lineR)
                lineHeures = remove_duplicated_text(lineHeures)
                lineT = remove_duplicated_text(lineT)
            else:
                lineOp = lineOp.replace("\n", "").strip()
                lineGuide = lineGuide.replace("\n", "").strip()
                lineMC = lineMC.replace("\n", "").strip()
                lineDes = lineDes.replace("\n", "").strip()
                lineMan = lineMan.replace("\n", "").strip()
                linePrix = linePrix.replace("\n", "").strip()
                lineAuj = lineAuj.replace("\n", "").strip()
                lineR = lineR.replace("\n", "").strip()
                lineHeures = lineHeures.replace("\n", "").strip()
                lineT = lineT.replace("\n", "").strip()

            if lData["sub_lines"]:
                sub_lines = lData["sub_lines"]
                lineDes += f"\n{sub_lines}"

            if any(
                [
                    lineOp,
                    lineGuide,
                    lineMC,
                    lineDes,
                    lineMan,
                    linePrix,
                    lineAuj,
                    lineR,
                    lineHeures,
                    lineT,
                ]
            ):
                lines.append(
                    {
                        "header": lData["part"],
                        "operation": lineOp if lineOp else "N/A",
                        "dbRef": lineGuide if lineGuide else "N/A",
                        "lineMC": lineMC if lineMC else "N/A",
                        "description": lineDes if lineDes else "N/A",
                        "Number": lineMan if lineMan else "N/A",
                        "TotalPrice": linePrix if linePrix else "N/A",
                        "lineAuj": lineAuj if lineAuj else "N/A",
                        "lineR": lineR if lineR else "N/A",
                        "TotalUnits": lineHeures if lineHeures else "N/A",
                        "Type": lineT if lineT else "N/A",
                    }
                )

    return lines


def remove_duplicates(s):
    half_len = len(s) // 2
    for i in range(1, half_len + 1):
        prefix = s[:i]
        repeated_part = s[i : i + len(prefix)]
        if prefix == repeated_part:
            return s[:i]
    return s


def lowercase_and_remove_spaces(input_str):
    string_replated = input_str.lower().replace(" ", "")
    string_replated = string_replated.replace("laborpart", "")
    return remove_duplicates(string_replated)


def check_pdf_type_format(pdf_document):
    totalDocumentPage = pdf_document.page_count
    pdf_type = "unknow_type"
    car_owner = car_ins = car_vin = car_odo = car_name = "N/A"
    first_time_see_line = False
    language = "en"

    for page_num in range(totalDocumentPage):
        page = pdf_document.load_page(page_num)
        text = page.get_text("text")

        if "Propriétaire:" in text and language == "en":
            language = "fr"

        # Remove content from the top to "LABOR PART"
        if "Line #" in text and first_time_see_line == False:

            labor_pos = page.search_for("Line #")[0]
            first_time_see_line = True
            table_heading = read_text_by_pos_mc(
                page, labor_pos.x0, labor_pos.y0, page.rect.width, labor_pos.y1
            )
            table_heading = lowercase_and_remove_spaces(table_heading)
            if (
                table_heading
                == "line#descriptionoperationtypetotalunitstypenumberqtytotalpricetax"
            ):
                pdf_type = "mitchell_type1_en"
            elif (
                table_heading
                == "line#descriptionoperationtypetotalunitscegtypenumberqtytotalpricetax"
            ):
                pdf_type = "mitchell_type2_en"

        if "MAIN-D'ŒUVRE" in text and "Ligne #" in text:
            labor_pos = page.search_for("Ligne #")[0]
            first_time_see_line = True
            table_heading = read_text_by_pos_mc(
                page, labor_pos.x0, labor_pos.y0, page.rect.width, labor_pos.y1
            )
            table_heading = lowercase_and_remove_spaces(table_heading)
            if (
                table_heading
                == "ligne#descriptionopérationtypeunitéstotalestypenuméroqtéprixtotaltaxe"
            ):
                pdf_type = "mitchell_type1_fr"
            elif (
                table_heading
                == "ligne#descriptionopérationtypeunitéstotalescegtypenuméroqtéprixtotaltaxe"
                or table_heading
                == "ligne#descriptionopérationtypeunitéscegtypenuméroqtéprixtotaltaxe"
            ):
                pdf_type = "mitchell_type2_fr"

        if "Audatex North America" in text or "AUDATEX" in text or "Audatex" in text:
            pdf_type = "audatex_" + language

    return pdf_type


def run(input_pdf_path, parent_folder):
    parent_path = create_unique_folder(parent_folder)
    pdf_document = fitz.open(input_pdf_path)
    pages_to_remove = []

    totalDocumentPage = pdf_document.page_count
    for page_num in range(totalDocumentPage):
        page = pdf_document.load_page(page_num)
        try:
            page.clean_contents()
        except:
            pages_to_remove.append(page_num)

    pages_to_remove.reverse()
    if len(pages_to_remove) > 0:
        for page_num in pages_to_remove:
            pdf_document.delete_page(page_num)
            output_pdf_path = os.path.join(parent_path, "cleaned_output.pdf")
            pdf_document.save(output_pdf_path)
            pdf_document.close()
            new_pdf_document = fitz.open(output_pdf_path)
    else:
        pdf_document.close()
        new_pdf_document = fitz.open(input_pdf_path)

    pdf_type = check_pdf_type_format(new_pdf_document)

    if pdf_type == "unknow_type":
        print("This PDF use different format, ignore this")
        return
    elif pdf_type == "mitchell_type1_en":
        car_data = get_estimate_information(new_pdf_document, "en")
        output_pdf_path = clean_pdf_mitchell(new_pdf_document, parent_path, "en")
        lines = read_text_mitchell_type_1(output_pdf_path, "en")
    elif pdf_type == "mitchell_type2_en":
        car_data = get_estimate_information(new_pdf_document, "en")
        output_pdf_path = clean_pdf_mitchell(new_pdf_document, parent_path, "en")
        lines = read_text_mitchell_type_2(output_pdf_path, "en")
    elif pdf_type == "mitchell_type1_fr":
        car_data = get_estimate_information(new_pdf_document, "fr")
        output_pdf_path = clean_pdf_mitchell(new_pdf_document, parent_path, "fr")
        lines = read_text_mitchell_type_1(output_pdf_path, "fr")
    elif pdf_type == "mitchell_type2_fr":
        car_data = get_estimate_information(new_pdf_document, "fr")
        output_pdf_path = clean_pdf_mitchell(new_pdf_document, parent_path, "fr")
        lines = read_text_mitchell_type_2(output_pdf_path, "fr")
    elif pdf_type == "audatex_fr":
        car_data = get_estimate_information_audatex(new_pdf_document, "fr")
        output_pdf_path = clean_pdf_audatex(new_pdf_document, parent_path, "fr")
        lines = read_text_audatex(output_pdf_path, "fr")
    elif pdf_type == "audatex_en":
        car_data = get_estimate_information_audatex(new_pdf_document, "en")
        output_pdf_path = clean_pdf_audatex(new_pdf_document, parent_path, "en")
        lines = read_text_audatex(output_pdf_path, "en")

    new_pdf_document.close()
    output = {
        "name": car_data["car_owner"],
        "vehicle_name": car_data["car_name"],
        "vin": car_data["car_vin"],
        "odometer": car_data["car_odo"],
        "insurance_company": car_data["car_ins"],
        "lines": lines,
        "type": pdf_type,
    }
    # json_output = json.dumps(output, indent=2)
    json_output = json.dumps(output)
    print(json_output)
    return json_output


def lambda_handler(event, context):
    # Getting the PDF file path from the event
    pdf_url = event["pdf_url"]
    file_name = os.path.basename(pdf_url) or "file.pdf"
    local_path = f"/tmp/{file_name}"

    # Download PDF and save locally
    response = requests.get(pdf_url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download PDF: {response.status_code}")

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    result = run(local_path, "/tmp")
    return json.loads(result)

