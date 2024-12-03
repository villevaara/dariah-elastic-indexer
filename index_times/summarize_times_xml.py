from lxml import etree
from glob import glob
from tqdm import tqdm
from collections import Counter


def strip_ns_prefix(tree):
    #xpath query for selecting all element nodes in namespace
    query = "descendant-or-self::*[namespace-uri()!='']"
    #for each element returned by the above xpath query...
    for element in tree.xpath(query):
        #replace element name with its local name
        element.tag = etree.QName(element).localname
    return tree


def get_root_child_tags(tree):
    # tree = strip_ns_prefix(etree.parse(file))
    root = tree.getroot()
    root_children_tags = [i.tag for i in root]
    return root_children_tags


def get_page_child_tags(tree):
    # tree = strip_ns_prefix(etree.parse(file))
    root = tree.getroot()
    root_pages = root.findall(".//page")
    child_tags = list()
    for p in root_pages:
        p_child_tags = [i.tag for i in p]
        child_tags.extend(p_child_tags)
    return child_tags


def get_meta_child_tags(tree):
    # tree = strip_ns_prefix(etree.parse(file))
    root = tree.getroot()
    meta_pages = root.findall(".//metadatainfo")
    child_tags = list()
    for p in meta_pages:
        p_child_tags = [i.tag for i in p]
        child_tags.extend(p_child_tags)
    return child_tags


fileroot = '../../dariah-elastic-data/times/tda/TDAO0001/TDAO0001-C00000/Newspapers/0FFO/'
all_files = glob(fileroot + '*.xml')

all_tags = list()
meta_tags = list()
page_tags = list()
xml_errors_counter = 0

for f in tqdm(all_files):
    try:
        tree = strip_ns_prefix(etree.parse(f))
        all_tags.extend(get_root_child_tags(tree))
        page_tags.extend(get_page_child_tags(tree))
        meta_tags.extend(get_meta_child_tags(tree))
    except etree.XMLSyntaxError:
        print('Skipping invalid XML file.')
        print(f)
        xml_errors_counter += 1

all_tags_counts = Counter(all_tags)
all_page_counts = Counter(page_tags)
all_meta_counts = Counter(meta_tags)

