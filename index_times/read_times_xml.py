from lxml import etree
from glob import glob
from tqdm import tqdm
from collections import Counter
import re


def strip_ns_prefix(tree):
    #xpath query for selecting all element nodes in namespace
    query = "descendant-or-self::*[namespace-uri()!='']"
    #for each element returned by the above xpath query...
    for element in tree.xpath(query):
        #replace element name with its local name
        element.tag = etree.QName(element).localname
    return tree


def get_field_text_if_exists(etree_elem, find_string):
    if etree_elem.find(find_string) is not None:
        if etree_elem.find(find_string).text != "":
            return etree_elem.find(find_string).text
    return None


def get_multifield_text_if_exists(etree_elem, find_string):
    if etree_elem.findall(find_string) is not None:
        all_fields =  etree_elem.findall(find_string)
        all_text = list()
        for field in all_fields:
            if field.text is not None:
                all_text.append(field.text.strip())
        this_text = "\n".join(all_text).strip()
        if this_text != "":
            return this_text
    return None


def get_issue_metadata(xml_root):
    meta = xml_root.find(".//metadatainfo")
    retmeta = dict()
    retmeta['newspaper_id'] = get_field_text_if_exists(meta, ".//newspaperID")
    retmeta['issue_publication_date'] = get_field_text_if_exists(meta, ".//da/searchableDateStart")
    retmeta['issue_page_count'] = get_field_text_if_exists(meta, ".//ip")
    # retmeta['issue_number'] = get_field_text_if_exists(meta, ".//is")
    retmeta['issue_id'] = get_field_text_if_exists(meta, ".//PSMID")
    retmeta['issue_publication_weekday'] = get_field_text_if_exists(meta, ".//dw")
    return retmeta


def test_hyphenated(token):
    # Return True if token ends with hyphen and char before that is a lowercase letter.
    match = re.search(r'[a-z]-', token)
    if match is not None:
        return True
    else:
        return False


def get_article_text(article_element):
    # <text.title>
    # <text.cr>
    # text.title together with rest of text, no additional markup: title already in article metadata
    # hierarchy: <text.cr/text.title> (join: "\n\n\n") > <p> (join: "\n\n") > <wd> (join: " ", try to merge hyphenated words)
    text_elements = article_element.xpath(".//text.title | .//text.cr")
    if len(text_elements) == 0:
        return None
    all_texts = list()
    for text_element in text_elements:
        elem_text = list()
        for p in text_element.findall(".//p"):
            joined_wds = ""
            p_texts = list(p.findall(".//wd"))
            # Only try to process words if there are any
            if len(p_texts) > 0:
                clear_wd = [wd.text.strip() for wd in p_texts if wd.text is not None] # Somehow all characters are not always read succesfully.
                last_wd_ind = len(clear_wd) - 1
                # test if item has last char '-' and the one before that a lowercase letter
                # test if item is the last in list.
                for i in range(0, len(clear_wd)):
                    this_wd = clear_wd[i]
                    if test_hyphenated(this_wd) and i != last_wd_ind:
                        joined_wds += this_wd[:-1]
                    else:
                        joined_wds += this_wd + " "
            joined_wds = joined_wds.strip()
            if len(joined_wds) > 0:
                elem_text.append(joined_wds)
        elem_text_joined = "\n\n".join(elem_text).strip()
        all_texts.append(elem_text_joined)
    article_text = "\n\n\n".join(all_texts).strip()
    return article_text


def get_article_authors(article_element):
    authors_elems = article_element.findall(".//detailed_au")
    if len(authors_elems) == 0:
        return []
    author_names = list()
    for author_e in authors_elems:
        if author_e.find(".//composed") is not None:
            author_names.append(author_e.find(".//composed").text.strip())
    return author_names


def get_article_data(article_element):
    retdata = dict()
    retdata['article_ocr_quality'] = get_field_text_if_exists(article_element, ".//ocr")
    retdata['article_id'] = get_field_text_if_exists(article_element, ".//id")
    retdata['article_type'] = article_element.get('type')
    retdata['article_title'] = get_multifield_text_if_exists(article_element, ".//ti")
    if retdata['article_title'] is not None:
        retdata['article_title'] = retdata['article_title'].strip('.,')
    retdata['article_title_additional'] = get_multifield_text_if_exists(article_element, ".//ta")
    if retdata['article_title_additional'] is not None:
        retdata['article_title_additional'] = retdata['article_title_additional'].strip('.,')
    retdata['section_title'] = get_multifield_text_if_exists(article_element, ".//ct")
    if retdata['section_title'] is not None:
        retdata['section_title'] = retdata['section_title'].strip('.,')
    retdata['article_page_count'] = get_field_text_if_exists(article_element, ".//pc")
    # number of illustration <il>
    # or: illustrations, with caption text, type, etc
    # Illustrations should probably be indexed as either nested or flattened object datatype. For now, just count the number
    retdata['article_illustration_count'] = len(article_element.findall('.//il'))
    retdata['article_authors'] = get_article_authors(article_element)
    retdata['article_authors_count'] = len(retdata['article_authors'])
    retdata['article_text'] = get_article_text(article_element)
    if retdata['article_text'] is not None:
        retdata['article_text_token_count'] = len(retdata['article_text'].split())
        retdata['article_text_char_count'] = len(" ".join(retdata['article_text'].split()))
    retdata['article_page_ids'] = [pi.text for pi in article_element.findall('.//pi')]
    retdata['article_page_numbers'] = [int(pi.attrib['pgref']) for pi in article_element.findall('.//pi')]
    return retdata


def set_empty_values(full_article_data):
    if full_article_data['article_ocr_quality'] is None:
        full_article_data['article_ocr_quality'] = -1.00
    else:
        full_article_data['article_ocr_quality'] = float(full_article_data['article_ocr_quality'])
    # set string fields
    for fieldname in ['article_title', 'article_title_additional', 'section_title', 'article_text', 'issue_publication_weekday',
                      'issue_id']:  
        if full_article_data[fieldname] is None:
            full_article_data[fieldname] = "[empty]"
        # Handle weekday problems
        elif fieldname == 'issue_publication_weekday':
            full_article_data[fieldname] = re.sub(',|\.|\[|\]', '', str(full_article_data[fieldname]))
    # set int fields
    for fieldname in ['article_page_count', 'article_illustration_count', 'article_authors_count', 'article_text_token_count',
                      'article_text_char_count', 'issue_page_count']:
        if full_article_data[fieldname] is None:
            full_article_data[fieldname] = -1
        else:
            full_article_data[fieldname] = int(re.sub(',|\.|\[|\]', '', str(full_article_data[fieldname]))) # Sometimes a number has , or . 
    # empty dates -> 99990101
    if full_article_data['issue_publication_date'] is None:
        full_article_data['issue_publication_date'] = "99990101"
    # leave empty lists empty. Or no?
    return full_article_data


def get_xml_file_articles(xml_file):
    try:
        tree = strip_ns_prefix(etree.parse(xml_file))
        root = tree.getroot()
        common_meta = get_issue_metadata(root)
        article_elements = root.findall(".//article")
        articles_processed = list()
        for article_element in article_elements:
            this_data = get_article_data(article_element)
            this_data.update(common_meta)
            full_article_data = set_empty_values(this_data)
            if full_article_data['article_id'] is None:
                pass
            if full_article_data['article_id'] == "":
                pass
            articles_processed.append(full_article_data)
        return articles_processed
    except etree.XMLSyntaxError:
        print('Invalid XML file.')
        return None






# fileroot = '../../dariah-elastic-data/times/tda/TDAO0001/TDAO0001-C00000/Newspapers/0FFO/'
# all_files = glob(fileroot + '*.xml')


# xml_file = all_files[0]
# xml_file_data = get_xml_file_articles(xml_file)


# tree = strip_ns_prefix(etree.parse(xml_file))
# root = tree.getroot()
# # meta = root.find(".//metadatainfo")
# pages = root.findall(".//page")



# TODO:
# Add default values for 


# input:
# xml files
# output:
# ndjson, 100 
# steps:
# 1. read metadata
# 2. read article data
