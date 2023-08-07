import json
from typing import Dict

from .doc_filter_functions import *
from indicnlp.tokenize.sentence_tokenize import sentence_split


def __get_lang_code(lang: str, lang_code: str):
    return ("urdu", "ur") if lang == "urdu" and lang_code == "urd" else (lang, lang_code)


def document_level_filters(data: Dict[str, str], lang:str, lang_code: str, ngram_start: int, ngram_end: int):
    # lid_res = (("English", {}), {})  # TODO: Call LID and get response
    # lang, _ = lid_res[0]
    lang, lang_code = __get_lang_code(lang, lang_code)
    word_distribution, word_segmented_data = calc_word_count(data["body"], lang_code)

    output = {
        "identifier": 0,
        "source": data["source"],
        "text": sentence_split(data["body"], lang_code),
        "word_distribution": word_distribution,
        "line_lengths": calc_line_lengths(data["body"], lang_code),
        "language_id": lang_code,
        "num_lines": calc_num_lines(data["body"], lang_code),
        "num_of_bad_unicodes": calc_incorrect_unicode(data["body"]),
        "char_distribution": calc_char_distribution(data["body"]),
        "nsfw_word_distribution": calc_nsfw_word_distribution(data["body"], lang),
        "repeated_line_distribution": calc_repeated_line_distribution(
            data["body"], lang_code
        ),
        "repeated_ngram_distribution": {
            "word": calculate_word_ngram_repetition(
                data["body"], lang_code, ngram_start, ngram_end
            ),
            "char": calculate_character_ngram_repetition(
                data["body"], ngram_start, ngram_end
            ),
        },
    }

    return output, word_segmented_data


def main():
    doc = {
        "title": "COVID-19 vaccination or test required for Ducks, Beavers games | 10tv.com",
        "body": "Published: 8:43 PM EDT August 20, 2021\nUpdated: 9:03 PM EDT August 20, 2021\nUniversity of Oregon (UO) and Oregon State (OSU) are requiring proof of vaccination or a negative COVID-19 test result for anyone 12 and older to attend designated university events and activities. And yes, that includes Ducks and Beavers games.\nThe policy takes effect Monday, Aug. 23. \nThose who are vaccinated can show their CDC-issued vaccination card , a cell phone picture of the card or a photocopy. Attendees must also be vaccinated at least two weeks past their second dose of the Pfizer or Moderna vaccines or first dose of Johnson & Johnson, the universities announced Friday. \nFor those without proof of vaccination, UO and OSU are requiring attendees to show documentation of a negative COVID-19 viral test taken within three days of an event. Home tests will not be accepted. \nThe documentation can be paper or electronic, and must include the following: \nTest result.\nType of test (e.g., nucleic acid amplification test or antigen test).\nEntity issuing the result (e.g., laboratory, health care entity or telehealth service).\nSpecimen collection date. A negative test result must show the specimen was collected within the three days before the event to be attended. \nAccompanying proof of identification in the form of a driver's license or other document that visibly identifies the person on the test result.\nFor UO events, those who've had a positive viral test within three months of an event, and who've met the criteria to end isolation, may obtain a letter from a health care provider saying they've been cleared for attendance. Oregon State confirmed this is not allowed under OSU's policy.\nUniversity officials hope the policy will help curb the spread of the fast-spreading delta variant, which has led to hospitals and emergency rooms being overwhelmed in recent weeks. \nRELATED: All Oregonians 5 and older required to wear face masks in indoor public places\nFor indoor events, attendees 5 and older will need to wear a face mask, even while seated, unless they are actively eating or drinking. This is in compliance with  the state's indoor mask mandate that went into effect Aug. 13 . \nAll seven of Oregon's public universities, including UO and OSU, are  requiring COVID vaccinations for students and staff this fall . \nIn Other News\n",
        "source": "10tv",
        "url": "https://www.10tv.com/article/news/health/coronavirus/vaccine/ducks-beavers-vaccinations-required/283-ff8d836f-18aa-4f47-b977-c9e1837df458",
        "timestamp": "15/11/21 22:45",
    }

    output = document_level_filters(doc, 1, 3)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
