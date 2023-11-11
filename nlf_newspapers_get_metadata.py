from smart_open import open
import json


def add_pagecounts(page_counts, result_item):
    if str(result_item['bindingId']) in page_counts.keys():
        result_item['pageCount'] = page_counts[str(result_item['bindingId'])]


def get_format_meta(media_format):
    main_listing_url = "https://digi.kansalliskirjasto.fi/api/dam/binding-search"
    this_request_url = main_listing_url + "?formats=" + media_format
    this_rows = list()
    while this_request_url is not None:
        with open(this_request_url, encoding='utf-8') as resp:
            results = json.load(resp)
            page_counts = results['bindingPageCounts']
            result_rows = results['rows']
            for item in result_rows:
                item.pop('pageNumber')
                item.pop("textHighlights")
                add_pagecounts(page_counts, item)
            this_rows.extend(result_rows)
            if len(result_rows) > 0:
                this_request_url = main_listing_url + "/" + results['scrollId']
            else:
                this_request_url = None
                print(media_format + " - DONE!")
            if len(this_rows) % 1000 == 0:
                print(media_format + " - " + str(len(this_rows)) + "/" + str(results['totalResults']))
    outfile = "data/nlf_" + media_format.lower() + "_meta.json"
    with open(outfile, 'w') as f:
        json.dump(this_rows, f, indent=4, ensure_ascii=False)
    return this_rows


# All formats:
# NEWSPAPER, JOURNAL, PRINTING, BOOK, MANUSCRIPT, MAP, MUSIC_NOTATION, PICTURE, CARD_INDEX

media_formats = ["JOURNAL", "NEWSPAPER"]

for mf in media_formats:
    get_format_meta(mf)


# def test_text_highlights(txt_h):
#     for v in txt_h.values():
#         if len(v) > 0:
#             print("foo")
#
#
# for item in this_rows:
#     test_text_highlights(item["textHighlights"])
