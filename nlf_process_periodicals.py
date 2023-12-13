# 1. read metadata from METS

# 2. read content from ALTO

from lxml import etree
from glob import glob


def get_block_text(block):
    block_text = list()
    for item in block.findall(".//String"):
        if 'SUBS_TYPE' in item.attrib.keys():
            if item.attrib['SUBS_TYPE'] == 'HypPart1':
                block_text.append(item.attrib['SUBS_CONTENT'])
            else:
                continue
        else:
            block_text.append(item.attrib['CONTENT'])
    return " ".join(block_text)


issue_path = "temp/38586/"


def get_issue_text(issue_path):
    pages = glob(issue_path + "alto/al-*.xml")
    pages.sort()
    page_texts = list()
    for page_path in pages:
        root = etree.parse(page_path).getroot()
        # Print the loaded XML
        page = root.find(".//Page")
        text_blocks = page.findall(".//TextBlock")
        page_text = list()
        for text_block in text_blocks:
            page_text.append(get_block_text(text_block))
        page_text_s = "\n\n".join(page_text)
        page_texts.append(page_text_s)
    page_texts_f = [text for text in page_texts if text != '']
    return "\n\n\n".join(page_texts_f)


iss_t = get_issue_text(issue_path)


with open('temp.txt', 'w') as outf:
    outf.write(iss_t)


# 1. read metadata json into memory.
# 2. take iteration n as parameter
# 3. bulk index in blocks of 10.000

with open("data/nlf_newspaper_meta.json", 'r') as jsonf:
    data = json.load(jsonf)
