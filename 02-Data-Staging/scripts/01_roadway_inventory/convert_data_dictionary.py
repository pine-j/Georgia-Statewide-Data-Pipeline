"""Convert the GDOT roadway data dictionary PDF into agent-friendly Markdown."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import fitz

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = PROJECT_ROOT / "01-Raw-Data" / "Roadway-Inventory"
PDF_PATH = RAW_DIR / "DataDictionary.pdf"
OUTPUT_MD = RAW_DIR / "DataDictionary.agent.md"
ASSET_DIR = RAW_DIR / "DataDictionary-assets"
PAGE_DIR = ASSET_DIR / "pages"
FIGURE_DIR = ASSET_DIR / "figures"

PAGE_SCALE = 2.0
PAGE_IMAGE_TEMPLATE = "page-{page:02d}.png"
FIGURE_IMAGE_TEMPLATE = "figure-{figure:02d}.png"
FULL_PAGE_FIGURE_PAGES = {12, 13, 16, 28, 30}

PAGE_TOPICS = {
    1: "Cover page",
    2: "Contents and contact information",
    3: "Purpose, background, and baseline data items",
    4: "Direction of inventory and inventory model diagrams",
    5: "County Code description and county codes (part 1)",
    6: "County codes (part 2)",
    7: "County codes (part 3) and omitted-number note",
    8: "Georgia Counties map example",
    9: "F_System",
    10: "Facility Type description, codes, and examples (part 1)",
    11: "Facility Type examples (part 2)",
    12: "GDOT Route Geometry description and Figure 12",
    13: "GDOT Route Geometry figures and route ID examples",
    14: "GDOT Route Geometry code tables",
    15: "GDOT Route Geometry examples",
    16: "Linear Referencing System diagram",
    17: "Median description, codes, and examples (part 1)",
    18: "Median examples (part 2)",
    19: "National Highway System",
    20: "Ownership",
    21: "Shoulder description and codes",
    22: "Shoulder examples (part 1)",
    23: "Shoulder examples (part 2)",
    24: "STRAHNET",
    25: "Surface description, codes, and examples (part 1)",
    26: "Surface examples (part 2)",
    27: "Through Lanes description",
    28: "Through Lanes example",
    29: "Urban Code",
    30: "Urban Areas example",
    31: "Year of Record",
    32: "Credits",
}

ROAD_INVENTORY_ITEMS = [
    ("County Code", "County_Code"),
    ("F_System", "Functional_Class"),
    ("Facility Type", "Operation"),
    ("GDOT Route Geometry", "Route_ID"),
    ("GDOT Route Geometry", "From_Measure"),
    ("GDOT Route Geometry", "To_Measure"),
    ("GDOT Route Geometry", "Section_Length"),
    ("Median", "Median_Type"),
    ("Median", "Median_Width"),
    ("National Highway System (NHS)", "NHS_Type"),
    ("Ownership", "Ownership"),
    ("Shoulder", "Shoulder_Type"),
    ("Shoulder", "Shoulder_Width_R"),
    ("Shoulder", "Shoulder_Width_L"),
    ("STRAHNET", "STRAHNET_Type"),
    ("Surface", "Surface_Type"),
    ("Through Lanes", "Lane_Width"),
    ("Through Lanes", "Total_Lanes"),
    ("Urban Code", "Urban_Code"),
    ("Year of Record", "Year_Record"),
]

F_SYSTEM_CODES = [
    ("1", "Interstate"),
    ("2", "Principal Arterial - Other Freeways and Expressways"),
    ("3", "Principal Arterial - Other"),
    ("4", "Minor Arterial"),
    ("5", "Major Collector"),
    ("6", "Minor Collector"),
    ("7", "Local"),
]

FACILITY_TYPES = [
    ("1", "One-Way (non-restricted)"),
    ("2", "Two-Way (non-restricted)"),
    ("3", "Ramp"),
    ("4", "Non Mainline"),
    ("5", "Non Inventory"),
    ("6", "Planned/Unbuilt"),
]

LRS_FUNCTION_TYPES = [
    ("1", "Main Line"),
    ("2", "Ramp"),
    ("3", "Collector Distributor"),
    ("4", "Ramp-CD Connector"),
    ("5", "Frontage Road"),
    ("7", "Separate Managed Facility"),
    ("8", "Local"),
    ("9", "Private"),
]

LRS_SYSTEM_CODES = [
    ("1", "State Highway Route"),
    ("2", "Public"),
    ("3", "Private"),
]

INTERSTATE_ROUTE_CODES = [
    ("I-16", "40400"),
    ("I-16 Spur", "404SP"),
    ("I-20", "40200"),
    ("I-24", "40900"),
    ("I-59", "40600"),
    ("I-59 Connector", "406CO"),
    ("I-75", "40100"),
    ("I-85", "40300"),
    ("I-95", "40500"),
    ("I-185", "41100"),
    ("I-285", "40700"),
    ("I-475", "40800"),
    ("I-516", "42100"),
    ("I-520", "41500"),
    ("I-575", "41700"),
    ("I-675", "41300"),
    ("I-985", "41900"),
]

ROUTE_SUFFIXES = [
    ("BY", "Bypass"),
    ("SP", "Spur"),
    ("AL", "Alternate"),
    ("BU", "Business"),
    ("CO", "Connector"),
    ("EA", "East"),
    ("EC", "East Connector"),
    ("LO", "Loop"),
    ("WE", "West"),
    ("SB", "South Business"),
    ("SE", "Spur East"),
    ("SO", "South"),
    ("NO", "North"),
    ("XL", "Express Lane"),
    ("XN", "Express Lane North of Atlanta"),
    ("XS", "Express Lane South of Atlanta"),
    ("XE", "Express Lane East of Atlanta"),
    ("XW", "Express Lane West of Atlanta"),
]

MEDIAN_TYPES = [
    ("1", "None - No median or unprotected area less than 4 feet wide."),
    ("2", "Unprotected - Median exists with a width of 4 feet or more."),
    ("3", "Curbed - Barrier or mountable curbs with a minimum height of 4 inches."),
    ("4", "Positive Barrier (unspecified) - Prevents vehicles from crossing median."),
    ("5", "Positive Barrier (flexible) - Considerable deflection upon impact."),
]

NHS_TYPES = [
    ("1", "Non Connector NHS"),
    ("2", "Major Airport"),
    ("3", "Major Port Facility"),
    ("4", "Major Amtrak Station"),
    ("5", "Major Rail/Truck Terminal"),
    ("6", "Major Inter City Bus Terminal"),
    ("7", "Major Public Transportation or Multi-Modal Passenger Terminal"),
    ("8", "Major Pipeline Terminal"),
    ("9", "Major Ferry Terminal"),
]

OWNERSHIP_TYPES = [
    ("1", "State DOT"),
    ("2", "County Highway Agency"),
    ("3", "Town or Township Highway Agency"),
    ("4", "City or Municipal Highway Agency"),
    ("11", "State Park, Forest or Reservation Agency"),
    ("12", "Local Park, Forest or Reservation Agency"),
    ("21", "Other State Agency"),
    ("25", "Other Local Agency"),
    ("26", "Private (other than Railroad)"),
    ("27", "Railroad"),
    ("31", "State Toll Road"),
    ("32", "Local Toll Authority"),
    ("40", "Other Public Instrumentality (i.e., Airport)"),
    ("50", "Indian Tribe Nation"),
    ("60", "Other Federal Agency"),
    ("62", "Bureau of Indian Affairs"),
    ("63", "Bureau of Fish and Wildlife"),
    ("64", "U.S. Forest Service"),
    ("66", "National Park Service"),
    ("67", "Tennessee Valley Authority"),
    ("68", "Bureau of Land Management"),
    ("69", "Bureau of Reclamation"),
    ("70", "Corps of Engineers"),
    ("72", "Air Force"),
    ("73", "Navy/Marines"),
    ("74", "Army"),
    ("80", "Other"),
]

SHOULDER_TYPES = [
    ("1", "None"),
    ("2", "Surface Shoulder Exists - Bituminous Concrete (AC)"),
    ("3", "Surface Shoulder Exists - Portland Cement Concrete (PCC) Surface"),
    (
        "4",
        "Stabilized Shoulder Exists - stabilized gravel or other granular material with or without admixture",
    ),
    ("5", "Combination Shoulder Exists - shoulder width has two or more surface types"),
    ("6", "Earth Shoulder Exists"),
    ("7", "Barrier Curb Exists; no shoulder in front of curb"),
]

STRAHNET_TYPES = [
    ("1", "Regular STRAHNET"),
    ("2", "Connector"),
]

SURFACE_TYPES = [
    ("1", "Unpaved"),
    ("2", "Bituminous"),
    ("3", "JPCP - Jointed Plain Concrete Pavement (includes whitetopping)"),
    ("5", "CRCP - Continuously Reinforced Concrete Pavement"),
    ("7", "AC Overlay over Existing Jointed Concrete Pavement"),
]

URBAN_CODES = [
    ("00901", "Albany"),
    ("03763", "Athens-Clarke County"),
    ("03817", "Atlanta"),
    ("04222", "Augusta-Richmond County"),
    ("11026", "Brunswick"),
    ("14185", "Cartersville"),
    ("15832", "Chattanooga"),
    ("19099", "Columbus"),
    ("22069", "Dalton"),
    ("32194", "Gainesville"),
    ("39133", "Hinesville"),
    ("52822", "Macon"),
    ("76204", "Rome"),
    ("79768", "Savannah"),
    ("89974", "Valdosta"),
    ("91783", "Warner Robins"),
    ("99998", "Small Urban Sections"),
    ("99999", "Rural Area Sections"),
]


def normalize_text(text: str) -> str:
    """Normalize PDF text into stable ASCII-friendly Markdown text."""
    replacements = {
        "\u2007": " ",
        "\u2009": " ",
        "\u202f": " ",
        "\u00a0": " ",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00a7": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def slugify(text: str) -> str:
    """Create a filesystem-safe slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "item"


def markdown_table(headers: list[str], rows: list[tuple[str, ...]]) -> str:
    """Render a Markdown table."""
    header_row = "| " + " | ".join(headers) + " |"
    divider_row = "| " + " | ".join("---" for _ in headers) + " |"
    body_rows = []
    for row in rows:
        cleaned = [str(value).replace("|", "\\|") for value in row]
        body_rows.append("| " + " | ".join(cleaned) + " |")
    return "\n".join([header_row, divider_row, *body_rows])


def format_rel(path: Path) -> str:
    """Return a POSIX-style relative path for Markdown."""
    return path.relative_to(RAW_DIR).as_posix()


def ensure_clean_output_dirs() -> None:
    """Reset the generated asset directories."""
    if ASSET_DIR.exists():
        shutil.rmtree(ASSET_DIR)
    PAGE_DIR.mkdir(parents=True, exist_ok=True)
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)


def horizontal_overlap(rect_a: fitz.Rect, rect_b: fitz.Rect) -> float:
    """Return the horizontal overlap width between two rectangles."""
    return max(0.0, min(rect_a.x1, rect_b.x1) - max(rect_a.x0, rect_b.x0))


def is_page_chrome(text: str) -> bool:
    """Return True for repeated page header/footer text that should not appear in figure crops."""
    if text in {
        "Georgia Department of Transportation",
        "Office of Transportation Data",
        "Georgia Department of Transportation Office of Transportation Data",
    }:
        return True
    if re.fullmatch(r"Page \d+ of \d+", text):
        return True
    if re.fullmatch(r"Last Updated: .* Page \d+ of \d+", text):
        return True
    if text.startswith("Last Updated: "):
        return True
    return False


def render_content_crop(
    page: fitz.Page,
    matrix: fitz.Matrix,
    figure_path: Path,
    text_blocks: list[dict[str, object]],
    image_rects: list[fitz.Rect],
) -> Path:
    """Render a page crop bounded by meaningful page content instead of page chrome."""
    page_rect = page.rect
    content_rect = None

    for image_rect in image_rects:
        if content_rect is None:
            content_rect = fitz.Rect(image_rect)
        else:
            content_rect.include_rect(image_rect)

    for block in text_blocks:
        block_rect = block["rect"]
        if content_rect is None:
            content_rect = fitz.Rect(block_rect)
        else:
            content_rect.include_rect(block_rect)

    if content_rect is None:
        content_rect = fitz.Rect(page_rect)

    content_rect.x0 = max(page_rect.x0, content_rect.x0 - 12)
    content_rect.y0 = max(page_rect.y0, content_rect.y0 - 12)
    content_rect.x1 = min(page_rect.x1, content_rect.x1 + 12)
    content_rect.y1 = min(page_rect.y1, content_rect.y1 + 12)

    pix = page.get_pixmap(matrix=matrix, clip=content_rect, alpha=False)
    pix.save(figure_path)
    return figure_path


def render_page_screenshots(doc: fitz.Document) -> dict[int, Path]:
    """Render every PDF page to a PNG screenshot."""
    page_images: dict[int, Path] = {}
    matrix = fitz.Matrix(PAGE_SCALE, PAGE_SCALE)
    for page_number in range(1, doc.page_count + 1):
        page = doc.load_page(page_number - 1)
        pix = page.get_pixmap(matrix=matrix, alpha=False)
        image_path = PAGE_DIR / PAGE_IMAGE_TEMPLATE.format(page=page_number)
        pix.save(image_path)
        page_images[page_number] = image_path
    return page_images


def extract_county_codes(doc: fitz.Document) -> list[tuple[str, str]]:
    """Extract the county/code lookup from the source PDF."""
    lines: list[str] = []
    for page_index in (4, 5, 6):
        text = doc.load_page(page_index).get_text("text")
        for raw_line in text.splitlines():
            line = normalize_text(raw_line)
            if line:
                lines.append(line)

    skip_lines = {
        "Last Updated: January 10, 2024",
        "Page 5 of 32",
        "Page 6 of 32",
        "Page 7 of 32",
        "Georgia Department of Transportation",
        "Office of Transportation Data",
        "3 County Code",
        "3.1 Description",
        "3.2 Event Table",
        "COUNTY",
        "3.34.3 Domain/Field Type",
        "County_Code: String",
        "3.4 Codes",
        "Table 2. County Codes",
        "County",
        "Code",
        "aNumbers 041 and 203 have been deliberately omitted.",
    }

    filtered: list[str] = []
    combine_statewide = False
    for line in lines:
        if line in skip_lines:
            continue
        if line == "MULTIPLE COUNTIES OR":
            filtered.append("MULTIPLE COUNTIES OR STATEWIDE")
            combine_statewide = True
            continue
        if combine_statewide and line == "STATEWIDE":
            combine_statewide = False
            continue

        line = re.sub(r"(?<=[A-Z])a$", "", line)
        if re.fullmatch(r"\d{3}", line):
            filtered.append(line)
            continue
        if re.fullmatch(r"[A-Z][A-Z\- ]+[A-Z]", line):
            filtered.append(line)

    pairs: list[tuple[str, str]] = []
    pending_name: str | None = None
    for item in filtered:
        if re.fullmatch(r"\d{3}", item):
            if pending_name:
                pairs.append((pending_name, item))
                pending_name = None
            continue
        pending_name = item

    if len(pairs) != 160:
        raise ValueError(f"Expected 160 county code rows, found {len(pairs)}")
    return pairs


def extract_figure_assets(
    doc: fitz.Document, page_images: dict[int, Path]
) -> dict[int, dict[str, object]]:
    """Create figure-level image assets where possible."""
    figure_assets: dict[int, dict[str, object]] = {}
    matrix = fitz.Matrix(PAGE_SCALE, PAGE_SCALE)

    for page_number in range(1, doc.page_count + 1):
        page = doc.load_page(page_number - 1)
        page_rect = page.rect
        page_width = page_rect.width
        page_height = page_rect.height

        raw_text_blocks = page.get_text("blocks")
        text_blocks: list[dict[str, object]] = []
        for block in raw_text_blocks:
            rect = fitz.Rect(block[:4])
            text = normalize_text(block[4])
            if not text:
                continue
            text_blocks.append({"rect": rect, "text": text})
        content_text_blocks = [
            block for block in text_blocks if not is_page_chrome(str(block["text"]))
        ]

        text_dict = page.get_text("dict")
        image_rects = []
        for block in text_dict.get("blocks", []):
            if block.get("type") != 1:
                continue
            rect = fitz.Rect(block["bbox"])
            if rect.get_area() < 5000:
                continue
            image_rects.append(rect)

        caption_blocks = []
        for block in content_text_blocks:
            match = re.match(r"^Figure\s+(\d+)\.", str(block["text"]))
            if match:
                caption_blocks.append(
                    {
                        "number": int(match.group(1)),
                        "text": block["text"],
                        "rect": block["rect"],
                    }
                )

        caption_blocks.sort(
            key=lambda item: (item["rect"].y0, item["rect"].x0)  # type: ignore[index]
        )

        if page_number in FULL_PAGE_FIGURE_PAGES:
            for caption in caption_blocks:
                figure_number = int(caption["number"])
                figure_path = FIGURE_DIR / FIGURE_IMAGE_TEMPLATE.format(figure=figure_number)
                render_content_crop(
                    page,
                    matrix,
                    figure_path,
                    content_text_blocks,
                    image_rects,
                )
                figure_assets[figure_number] = {
                    "page": page_number,
                    "caption": caption["text"],
                    "image": figure_path,
                    "mode": "page",
                }
            continue

        used_image_indexes: set[int] = set()
        for idx, caption in enumerate(caption_blocks):
            figure_number = int(caption["number"])
            caption_rect = caption["rect"]
            next_caption_y0 = page_height + 1
            if idx + 1 < len(caption_blocks):
                next_caption_y0 = caption_blocks[idx + 1]["rect"].y0

            best_image_index = None
            best_cost = None
            caption_center = caption_rect.tl + (caption_rect.br - caption_rect.tl) / 2
            for image_index, image_rect in enumerate(image_rects):
                if image_index in used_image_indexes:
                    continue
                image_center = image_rect.tl + (image_rect.br - image_rect.tl) / 2
                cost = (
                    abs(caption_center.x - image_center.x) / page_width
                    + abs(caption_center.y - image_center.y) / page_height
                )
                if best_cost is None or cost < best_cost:
                    best_cost = cost
                    best_image_index = image_index

            clip_rect = None
            if best_image_index is not None and best_cost is not None and best_cost < 1.5:
                image_rect = image_rects[best_image_index]
                used_image_indexes.add(best_image_index)
                crop_rect = fitz.Rect(image_rect)
                crop_rect.include_rect(caption_rect)

                for block in content_text_blocks:
                    block_rect = block["rect"]
                    if block_rect == caption_rect:
                        continue
                    if block_rect.y0 < caption_rect.y1 - 1:
                        continue
                    if block_rect.y1 > next_caption_y0 + 5:
                        continue
                    if (
                        horizontal_overlap(block_rect, image_rect) <= 0
                        and horizontal_overlap(block_rect, caption_rect) <= 0
                    ):
                        continue
                    crop_rect.include_rect(block_rect)

                crop_rect.x0 = max(page_rect.x0, crop_rect.x0 - 12)
                crop_rect.y0 = max(page_rect.y0, crop_rect.y0 - 12)
                crop_rect.x1 = min(page_rect.x1, crop_rect.x1 + 12)
                crop_rect.y1 = min(page_rect.y1, crop_rect.y1 + 12)

                if crop_rect.width < page_width * 0.95 or crop_rect.height < page_height * 0.95:
                    clip_rect = crop_rect

            if clip_rect is not None:
                figure_path = FIGURE_DIR / FIGURE_IMAGE_TEMPLATE.format(figure=figure_number)
                pix = page.get_pixmap(matrix=matrix, clip=clip_rect, alpha=False)
                pix.save(figure_path)
                figure_assets[figure_number] = {
                    "page": page_number,
                    "caption": caption["text"],
                    "image": figure_path,
                    "mode": "crop",
                }
            else:
                figure_path = FIGURE_DIR / FIGURE_IMAGE_TEMPLATE.format(figure=figure_number)
                render_content_crop(
                    page,
                    matrix,
                    figure_path,
                    content_text_blocks,
                    image_rects,
                )
                figure_assets[figure_number] = {
                    "page": page_number,
                    "caption": caption["text"],
                    "image": figure_path,
                    "mode": "page",
                }

    return figure_assets


def figure_markdown(
    figure_assets: dict[int, dict[str, object]],
    figure_numbers: list[int],
) -> str:
    """Render a figure gallery subsection."""
    parts = []
    for number in figure_numbers:
        if number not in figure_assets:
            continue
        asset = figure_assets[number]
        caption = str(asset["caption"])
        page_number = int(asset["page"])
        image_path = format_rel(Path(asset["image"]))
        parts.append(f"#### Figure {number}")
        parts.append(caption)
        parts.append(f"Source page: {page_number}")
        parts.append(f"![Figure {number}]({image_path})")
        parts.append("")
    return "\n".join(parts).strip()


def page_index_markdown(page_images: dict[int, Path]) -> str:
    """Build a page screenshot index table."""
    rows = []
    for page_number, image_path in page_images.items():
        rows.append(
            (
                str(page_number),
                PAGE_TOPICS.get(page_number, ""),
                f"[page {page_number}]({format_rel(image_path)})",
            )
        )
    return markdown_table(["Page", "Topic", "Screenshot"], rows)


def section_anchor(title: str) -> str:
    """Create a section anchor."""
    return slugify(title)


def build_markdown(
    county_codes: list[tuple[str, str]],
    figure_assets: dict[int, dict[str, object]],
    page_images: dict[int, Path],
) -> str:
    """Assemble the final agent-friendly Markdown document."""
    toc_rows = [
        ("1", "Purpose", "3"),
        ("2", "Background", "3"),
        ("3", "County Code", "5"),
        ("4", "F_System", "9"),
        ("5", "Facility Type", "10"),
        ("6", "GDOT Route Geometry", "12"),
        ("7", "Linear Referencing System", "16"),
        ("8", "Median", "17"),
        ("9", "National Highway System", "19"),
        ("10", "Ownership", "20"),
        ("11", "Shoulder", "21"),
        ("12", "STRAHNET", "24"),
        ("13", "Surface", "25"),
        ("14", "Through Lanes", "27"),
        ("15", "Urban Code", "29"),
        ("16", "Year of Record", "31"),
        ("17", "Credits", "32"),
    ]

    lines = [
        "# GDOT Roads & Highways Data Dictionary (Agent-Friendly Markdown Conversion)",
        "",
        "Source document metadata:",
        f"- Source PDF: [DataDictionary.pdf]({PDF_PATH.name})",
        "- Source title: Roads & Highways Data Dictionary document",
        "- Source author: GDOT OTD",
        "- Source creation date: 2024-01-10",
        "- Source last updated text in PDF: January 10, 2024",
        "- PDF page count: 32",
        "- Generated by: `02-Data-Staging/scripts/01_roadway_inventory/convert_data_dictionary.py`",
        "",
        "## How To Use This File",
        "",
        "- This Markdown is organized for analysis rather than print layout.",
        "- Each domain section includes the event table, field type(s), code table, interpretation notes, and figure references.",
        "- Full-page screenshots are available for every source page under `DataDictionary-assets/pages/`.",
        "- Figure crops are provided where the PDF contained discrete example images; complex diagram pages fall back to the full-page screenshot.",
        "",
        "## Original PDF Contents",
        "",
        markdown_table(["Section", "Title", "Starts On PDF Page"], toc_rows),
        "",
        "## Core Document Context",
        "",
        "### Purpose",
        "",
        "GDOT's Office of Transportation Data (OTD) publishes this data dictionary as the companion reference for the roadway inventory dataset. It defines the roadway characteristics, identifies the relevant network event tables, and provides examples and coding guidance for the main baseline attributes.",
        "",
        "### Background And Modeling Conventions",
        "",
        "- OTD maintains inventory data for more than 125,000 centerline miles of public roads in Georgia.",
        "- The source collection methods called out in the PDF are remote sensing, Local Road Activity (LRA) reports, construction design plans, and data mining.",
        "- Direction of inventory runs south-to-north and west-to-east.",
        "- The side of the road that matches the direction of inventory is the inventory side.",
        "- Exit numbers and route mileage follow the same directional convention. The PDF uses I-75 as the example: Mile Post 1 is near Florida and Mile Post 354 is near Tennessee.",
        "- GDOT uses ArcGIS, linear referencing, and dynamic segmentation to build route geometry and to place network events on those routes.",
        "- Road inventory event attributes can be linear events (for example surface, shoulder, ownership) or point events (for example bridges, railroad crossings, traffic control).",
        "- The source PDF states that the published road inventory is the most current data available and is provided as-is.",
        "",
        "### Baseline Data Items",
        "",
        markdown_table(["Category", "Baseline Data Item"], ROAD_INVENTORY_ITEMS),
        "",
        "### Visual References For Global Concepts",
        "",
        figure_markdown(figure_assets, [1, 2, 3]),
        "",
        "## Section 3: County Code",
        "",
        f"Anchor: `{section_anchor('County Code')}`",
        "",
        "Description:",
        "- Georgia is divided into 159 counties.",
        "- County codes are odd numbers from `001` through `321`.",
        "- The PDF explicitly notes that codes `041` and `203` are deliberately omitted.",
        "- A special value of `000` means multiple counties or statewide.",
        "",
        "Event table: `COUNTY`",
        "",
        "Field type:",
        "- `County_Code`: `String`",
        "",
        "Codes:",
        "",
        markdown_table(["County", "Code"], county_codes),
        "",
        "Visual reference:",
        "",
        figure_markdown(figure_assets, [4]),
        "",
        "## Section 4: F_System",
        "",
        "Description:",
        "- `F_System` groups streets and highways into functional classes based on the service they provide in the overall road network.",
        "- The document notes that comprehensive system updates occur about every 10 years and are tied to U.S. Census updates.",
        "- The PDF notes that `F_System` may also be called functional classification.",
        "- Reference cited in the PDF: FHWA Highway Performance Monitoring System (HPMS) Field Manual, Section 4.4, Data Item 1.",
        "",
        "Event table: `FUNCTIONAL_CLASS`",
        "",
        "Field type:",
        "- `Functional_Class`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], F_SYSTEM_CODES),
        "",
        "## Section 5: Facility Type",
        "",
        "Description:",
        "- `Facility Type` describes direction of travel or operational flow on the road segment.",
        "- The coding distinguishes one-way roads, two-way roads, ramps, non-mainline facilities, non-inventory facilities, and planned/unbuilt facilities.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 3.",
        "",
        "Event table: `OPERATION`",
        "",
        "Field type:",
        "- `Operation`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], FACILITY_TYPES),
        "",
        "Examples:",
        "",
        figure_markdown(figure_assets, [5, 6, 7, 8, 9, 10, 11]),
        "",
        "## Section 6: GDOT Route Geometry",
        "",
        "Description:",
        "- `Route_ID` is the key route identifier for the roadway inventory.",
        "- The PDF says each route ID is composed of six sub-types: Function Type, County, System Code, Route Code, Route Suffix, and Direction.",
        "- `From_Measure` and `To_Measure` define the beginning and end of the road segment.",
        "- `Section_Length` is the calculated segment length from the measures.",
        "",
        "Event table: `LRSN_GDOT`",
        "",
        "Field types:",
        "- `Route_Code`: `String`",
        "- `From_Measure`: `Dynamic Segmentation`",
        "- `To_Measure`: `Dynamic Segmentation`",
        "- `Section_Length`: `Dynamic Segmentation`",
        "- `Route_Function`: `String`",
        "- `System_Code`: `String`",
        "- `Direction`: `String`",
        "",
        "Route ID interpretation notes from the PDF:",
        "- Function Type describes how the road functions: mainline, ramp, collector distributor, ramp-CD connector, frontage road, alley, managed facility, Y connector, private road, or projected route.",
        "- County Code refers back to Section 3.",
        "- System Code is the simplified identifier for state routes, public roads, and private roads.",
        "- Route Code identifies routes numerically. The PDF notes that interstates and other freeways use route numbers in the 400s and 500s.",
        "- Route Suffix is a two-character abbreviation.",
        "- Direction is the inventory direction, usually `INC` or `DEC`.",
        "",
        "LRS Function Type codes:",
        "",
        markdown_table(["Code", "Description"], LRS_FUNCTION_TYPES),
        "",
        "LRS System Code values:",
        "",
        markdown_table(["Code", "Description"], LRS_SYSTEM_CODES),
        "",
        "Interstate designations and state route codes:",
        "",
        markdown_table(["Interstate Designation", "State Route Code"], INTERSTATE_ROUTE_CODES),
        "",
        "Route suffix values:",
        "",
        markdown_table(["Code", "Description"], ROUTE_SUFFIXES),
        "",
        "Important route-format details from Figure 13:",
        "- County digits `000` mean statewide or multiple counties for state routes only.",
        "- County values `001`-`321` map to county FIPS-style county codes used by GDOT.",
        "- The route code is six characters long.",
        "- The route suffix is two characters long.",
        "- For legacy routes, the figure says the last two characters are used for suffixes and city codes and should otherwise stay `00`.",
        "- For ramps, collector distributors, and ramp-CD connectors (function types `2`-`4`), digits 6-8 are reference post, digits 9-11 are route number, digit 12 is quadrant `A-Z`, and digit 13 is route suffix or exit number using `0` for No or `1` for Yes.",
        "- Example route IDs shown in the source PDF:",
        "  - `1000100040100INC` = I-75 North Increasing",
        "  - `70671000401XNINC` = I-75 Northwest Corridor Express Lanes",
        "  - `1000100009200INC` = GA 92 through multiple counties",
        "  - `1135200008300INC` = Local Road - Buford Dam Rd NE - Gwinnett County",
        "  - `10151000020SPINC` = GA 20 Spur - Bartow County",
        "",
        "Reference cited in the PDF:",
        "- GDOT Understanding LRS Route IDs & RCLINK Route IDs guide: `https://www.dot.ga.gov/DriveSmart/Data/Documents/Guides/UnderstandingLRSID_RCLINKID_Doc.pdf`",
        "",
        "Figures and examples:",
        "",
        figure_markdown(figure_assets, [12, 13, 14, 15, 16, 17, 18, 19]),
        "",
        "## Section 7: Linear Referencing System",
        "",
        "The source PDF dedicates this section to a visual diagram only. The broader linear-referencing concepts are described in the background and route-geometry sections above.",
        "",
        "Visual reference:",
        "",
        figure_markdown(figure_assets, [20]),
        "",
        "## Section 8: Median",
        "",
        "Description:",
        "- A median is the part of a divided highway that separates opposing travel directions.",
        "- `Median_Type` describes the separator or barrier style.",
        "- `Median_Width` is measured between the inside edges of the left-most through lane in each direction and rounded to the nearest foot.",
        "- The PDF notes that medians do not include shoulders.",
        "- The PDF also notes that barriers are always located within the median and that barrier type can affect the median code used for reporting.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Items 35 and 36.",
        "",
        "Event tables:",
        "- `Median`",
        "- `Barrier`",
        "",
        "Field types:",
        "- `Median_Type`: `Numeric`",
        "- `Median_Width`: numeric value `0`-`99`, rounded to the nearest foot",
        "",
        markdown_table(["Code", "Description"], MEDIAN_TYPES),
        "",
        "Examples:",
        "",
        figure_markdown(figure_assets, [21, 22, 23, 24, 25]),
        "",
        "## Section 9: National Highway System",
        "",
        "Description:",
        "- The National Highway System (NHS) contains roadways important to national economy, defense, and mobility.",
        "- The PDF frames the modern NHS as the post-Interstate Federal-aid system established under the Intermodal Surface Transportation Efficiency Act of 1991.",
        "- The PDF explicitly notes that STRAHNET and STRAHNET connectors are covered separately in Section 12.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 64.",
        "",
        "Event table: `NHS`",
        "",
        "Field type:",
        "- `NHS_Type`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], NHS_TYPES),
        "",
        "## Section 10: Ownership",
        "",
        "Description:",
        "- `Ownership` is the legal responsibility and jurisdiction over the roadway and rights-of-way.",
        "- A road can be physically located in one county or GDOT field district but legally owned or maintained by another entity.",
        "- The PDF also notes that some Georgia county governments are consolidated with city governments.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 6.",
        "",
        "Event table: `LRS.LRSE_OWNERSHIP`",
        "",
        "Field type:",
        "- `Type of Ownership`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], OWNERSHIP_TYPES),
        "",
        "## Section 11: Shoulder",
        "",
        "Description:",
        "- Shoulders extend from the through lane to the edge of the roadway surface materials.",
        "- `Shoulder_Type` captures the predominant shoulder condition in the inventory direction.",
        "- `Shoulder_Width_R` and `Shoulder_Width_L` are rounded to the nearest foot.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Items 37, 38, and 39.",
        "",
        "Measurement rules from the PDF:",
        "- Left shoulder: measure from the outer edge of the left-most through lane to the left-most edge of the inside shoulder.",
        "- Right shoulder: measure from the outer edge of the right-most through lane to the outer edge of the shoulder.",
        "- If rumble strips are present, code the full shoulder width.",
        "- If abutting parking or a bike lane occupies the space, shoulder width is zero.",
        "- If a bike lane exists, include only the shoulder width and exclude the bike lane width.",
        "- For earth shoulders, measure from the white stripe to the shoulder break point.",
        "- For shoulders with guardrail, measure from the through-lane edge to the face of the guardrail.",
        "",
        "Event tables:",
        "- `SHOULDER_TYPE`",
        "- `SHOULDER_WIDTH`",
        "",
        "Field types:",
        "- `Shoulder Type`: `Numeric`",
        "- `Shoulder Width`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], SHOULDER_TYPES),
        "",
        "Examples:",
        "",
        figure_markdown(figure_assets, [26, 27, 28, 29, 30, 31, 32]),
        "",
        "## Section 12: STRAHNET",
        "",
        "Description:",
        "- `STRAHNET` identifies roadway sections on the Strategic Highway Network.",
        "- Code `1` means a regular STRAHNET segment.",
        "- Code `2` means a major STRAHNET connector between major military installations and STRAHNET highways.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 65.",
        "",
        "Event table: `STRAHNET`",
        "",
        "Field type:",
        "- `STRAHNET_Type`: `Small Integer`",
        "",
        markdown_table(["Code", "Description"], STRAHNET_TYPES),
        "",
        "## Section 13: Surface",
        "",
        "Description:",
        "- `Surface` describes the roadway material that carries vehicle traffic.",
        "- The PDF distinguishes asphalt, Portland cement concrete, gravel, and other unpaved surface categories used for low-volume roads.",
        "- Markings used to guide traffic are part of the surfaced roadway context, except for gravel and other unpaved types.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 49.",
        "",
        "Event table: `SURFACE`",
        "",
        "Field type:",
        "- `Surface_Type`: `Numeric`",
        "",
        markdown_table(["Code", "Description"], SURFACE_TYPES),
        "",
        "Examples:",
        "",
        figure_markdown(figure_assets, [33, 34, 35, 36, 37]),
        "",
        "## Section 14: Through Lanes",
        "",
        "Description:",
        "- `Through Lanes` counts lanes designated for through traffic, separately by direction.",
        "- The PDF's example says a common 4-lane two-way road is coded as 2 lanes increasing and 2 lanes decreasing.",
        "- Turn lanes, auxiliary lanes, and collector distributor lanes are excluded.",
        "- `Lane_Width` is rounded to the nearest foot.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 49.",
        "",
        "Event table: `THROUGH_LANE`",
        "",
        "Field types:",
        "- `Total_Lanes`: `Small Integer`",
        "- `Lane_Width`: `Small Integer`",
        "",
        "Codes:",
        "- Both values are stored as integers.",
        "",
        "Example:",
        "",
        figure_markdown(figure_assets, [38]),
        "",
        "## Section 15: Urban Code",
        "",
        "Description:",
        "- Urban codes follow the FHWA HPMS framework and are tied to the urban/rural context for functional classification.",
        "- Urban Area Boundaries (UABs) are based on the U.S. Census Bureau's 10-year census cycle.",
        "- The UAB is the boundary between urban and rural functional classification in Georgia.",
        "- Reference cited in the PDF: FHWA HPMS Field Manual, Section 4.4, Data Item 49.",
        "",
        "Event table: `URBAN_CODE`",
        "",
        "Field type:",
        "- `Urban_Code`: `String`",
        "",
        markdown_table(["Code", "Description"], URBAN_CODES),
        "",
        "Example:",
        "",
        figure_markdown(figure_assets, [39]),
        "",
        "## Section 16: Year of Record",
        "",
        "Description:",
        "- `Year_Record` is the calendar year when the data was created or updated.",
        "",
        "Event table: `YEAR_RECORD`",
        "",
        "Field type:",
        "- `Year_Record`: `Small Integer`",
        "",
        "Codes:",
        "- The PDF says the values are literal calendar years, for example `2019`.",
        "",
        "## Section 17: Credits",
        "",
        "Authors listed in the source PDF:",
        "- Sarah Gitt, GDOT Program Manager (former)",
        "- Jennifer Heitert, GDOT Program Manager",
        "- Danielle Mallon, GDOT Program Manager (former)",
        "- Kiisa Wiegand, Business Analyst",
        "",
        "## Full Page Screenshot Index",
        "",
        page_index_markdown(page_images),
        "",
        "## Asset Locations",
        "",
        f"- Full-page screenshots: `{format_rel(PAGE_DIR)}`",
        f"- Figure crops and figure-page fallbacks: `{format_rel(FIGURE_DIR)}`",
    ]
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    """Run the PDF-to-Markdown conversion."""
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"Source PDF not found: {PDF_PATH}")

    ensure_clean_output_dirs()

    with fitz.open(PDF_PATH) as doc:
        page_images = render_page_screenshots(doc)
        county_codes = extract_county_codes(doc)
        figure_assets = extract_figure_assets(doc, page_images)
        markdown = build_markdown(county_codes, figure_assets, page_images)

    OUTPUT_MD.write_text(markdown, encoding="utf-8")


if __name__ == "__main__":
    main()
