import pandas as pd
import re
import pytest
import json


def normalize_text(text):
    """Приведение строки к нижнему регистру, удаление лишних пробелов."""
    if not isinstance(text, str):
        return None
    return text.lower().strip()

def standardize_field(value, replace_dict: dict):
    """Нормализация названия поля."""
    value = normalize_text(value)
    if value is None:
        return 'N\A'
    
    for pattern, replacement in replace_dict.items():
        if re.search(re.escape(pattern.lower()), value):
            return replacement
    
    return 'N\A'


def process_courses(df: pd.DataFrame):
    """Применяет нормализацию к DataFrame."""
    with open('course_replacements.json') as fp1, open('subject_replacements.json') as fp2:
        course_replacements = json.load(fp1)
        subject_replacements = json.load(fp2)
        
    # По производительности: apply работает медленнее стандартных методов pandas(например replace), 
    # но зато позволяет обрабатываеть строки более гибко.
    
    df['standardized_course'] = df['course'].apply(standardize_field, args=([course_replacements]))
    df['standardized_subject'] = df['subject'].apply(standardize_field, args=([subject_replacements]))
    return df


def test_standardization():
    replace_dict_course = {
        
        "в погоне за соткой 2к24": "Полугодовой курс",
        "курс pro": "Спецкурс"
    }
    
    replace_dict_subject = {
        "математикаогэ": "Математика ОГЭ",
        "английский": "Английский язык"
    }

    assert standardize_field('В погоне за соткой 2к24', replace_dict_course) == 'Полугодовой курс'
    assert standardize_field('Курс PRO', replace_dict_course) == 'Спецкурс'
    assert standardize_field(None, replace_dict_course) == 'N\A'
    assert standardize_field('МатематикаОГЭ', replace_dict_subject) == 'Математика ОГЭ'
    assert standardize_field('Английский', replace_dict_subject) == 'Английский язык'
    assert standardize_field(None, replace_dict_subject) == 'N\A'
    


if __name__ == '__main__':
    
    data = {'course': ['В погоне за соткой 2к24', 'Курс PRO', None], 'subject': ['МатематикаОГЭ', 'Английский', None]}
    df = pd.DataFrame(data)
    df = process_courses(df)
    print(df)



