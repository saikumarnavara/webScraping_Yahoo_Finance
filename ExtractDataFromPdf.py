
import pandas as pd
from io import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

def extract_text_from_pdf(pdf_path):
    resource_manager = PDFResourceManager()
    fake_file_handle = StringIO()
    converter = TextConverter(resource_manager, fake_file_handle, laparams=LAParams())
    page_interpreter = PDFPageInterpreter(resource_manager, converter)

    with open(pdf_path, 'rb') as fh:
        for page in PDFPage.get_pages(fh, 
                                      caching=True,
                                      check_extractable=True):
            page_interpreter.process_page(page)

        text = fake_file_handle.getvalue()

    # close open handles
    converter.close()
    fake_file_handle.close()

    if text:
        return text

pdf_text = extract_text_from_pdf(r'C:\Users\Jawahar\Desktop\pdf_2022.pdf')


def parse_text_data(text_data):
    lines = text_data.split('\n')
    data = []
    for line in lines:
        line_data = line.split()
        data.append(line_data)

    df = pd.DataFrame(data)
    df.to_csv("pdf_to_Df.csv")
    return df

df = parse_text_data(pdf_text)
