import re

class raw_credit_report_data_extraction():
    def __init__(self):
        self.regex_payment = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-]?\$\d{1,1000}\.\d{1,1000}')
        self.regex_transaction = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n\d{1}%\n[-+]?\$\d{1,1000}\.\d{1,1000}\n[-+]?\$\d{1,1000}\.\d{1,1000}')
        self.regex_installments = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-+]?\$\d{1,1000}\.\d{1,1000}\s\nTRANSACTION\s.*')
    
    def extract(self, my_credit_report):
        holder_payment = []
        holder_transaction = []
        holder_installments = []

        for page in range(len(my_credit_report.pages)):
            page_content = my_credit_report.pages[page].extract_text().replace(',','')

            holder_installments.append(regex_installments.findall(page_content))
            page_content = regex_installments.sub('', page_content)

            holder_transaction.append(regex_transaction.findall(page_content))
            page_content = regex_transaction.sub('', page_content)

            holder_payment.append(regex_payment.findall(page_content))
            page_content = regex_payment.sub('', page_content)

        holder_payment = [y for x in holder_payment for y in x]
        holder_transaction = [y for x in holder_transaction for y in x]
        holder_installments = [y for x in holder_installments for y in x]

        return (holder_payment, holder_transaction, holder_installments)