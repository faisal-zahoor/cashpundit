from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from erpnext.accounts.report.accounts_receivable.accounts_receivable import execute
from erpnext.accounts.report.accounts_payable.accounts_payable import execute as execute_ap
from datetime import datetime


def create_data():
	count = 0
	item  = list(set([i[0] for i in frappe.db.sql('''select i.name as item from `tabItem` as i inner join `tabBin` as b where b.warehouse = "Stores - VP" and b.actual_qty > 0''',as_list=1)]))
	customer = frappe.db.sql('''select name from `tabCustomer`''',as_dict = 1)
	for j in range(len(customer)):
		try:
			if  count == 100:
				break
			doc = frappe.new_doc("Sales Invoice")
			doc.naming_series = "SINV-.########"
			doc.customer = customer[j]['name']
			doc.append("items",{
				"item_code":item[j],
				"qty":2,
				"item_name":frappe.db.get_value("Item",item[j],"item_name"),
				"rate":150
			})
			doc.save(ignore_permissions = True)
			doc.submit()
			if doc.name:
				new_doc = frappe.new_doc("Payment Entry")
				new_doc.naming_series = "ACC-PAY-.YYYY.-"
				new_doc.payment_type = "Receive"
				new_doc.mode_of_payment = "Cash"
				new_doc.paid_to = "Cash - VP"
				new_doc.append("references",{
					"reference_doctype":"Sales Invoice",
					"reference_name":doc.name,
					"allocated_amount":doc.grand_total
				})
				new_doc.reference_no = "63683832"
				new_doc.reference_date = "2022-06-07"
				new_doc.paid_amount = doc.grand_total
				new_doc.received_amount = doc.grand_total
				print(doc.grand_total)
				new_doc.party_type = "Customer"
				new_doc.party = doc.customer
				new_doc.received_amount = doc.grand_total
				new_doc.save(ignore_permissions = True)
				new_doc.submit()
				if new_doc.name:
					print(doc.name,new_doc.name)
					count+=1
		except Exception as e:
			print(str(e))
			continue


@frappe.whitelist()
def get_company(name=None):
	l=[]
	filters =""
	if name:
		filters = "where name = \"{0}\" ".format(name)
	
	company_list = frappe.db.sql(''' select name as company from `tabCompany` {0} order by creation'''.format(filters), as_dict=1)
	for j in company_list:
		resp_dict = {}
		resp_dict["CompanyName"]=j["company"]
		c = frappe.db.get_list("Company",{"name":j["company"]},["abbr","default_currency","date_of_incorporation","email","creation","modified","contact_person"])
		addr = frappe.db.get_list("Address",{"name":frappe.db.get_value("Dynamic Link",{"link_doctype":"Company","link_name":j["company"]},"parent")},["address_line1","state","country"])
		resp_dict["CompanyCode"]=c[0]["abbr"] if len(c) else ''
		resp_dict["HomeCurrency"]=c[0]["default_currency"] if len(c) else ''
		resp_dict["StartDate"]=c[0]["date_of_incorporation"]
		resp_dict["Address"]= addr[0]["address_line1"] if len(addr) else ''
		resp_dict["State"]=addr[0]["state"] if len(addr) else ''
		resp_dict["Country"]=addr[0]["country"] if len(addr) else ''
		resp_dict["ContactPerson"]=c[0]["contact_person"] if len(c) else ''
		resp_dict["Email"]=c[0]["email"] if len(c) else ''
		resp_dict["CreatedDateTime"]=c[0]["creation"] if len(c) else ''
		resp_dict["ModifiedDateTime"]=c[0]["modified"] if len(c) else ''
		l.append(resp_dict)
	return l

@frappe.whitelist()
def get_bankdetail(name=None):
	l=[]
	filters=""
	if name:
		filters = "where name = \"{0}\" ".format(name)
	
	company_list = frappe.db.sql(''' select name as company from `tabCompany` {0}'''.format(filters), as_dict=1)
	for j in company_list:
		resp_dict = {}
		resp_dict["CompanyCode"]=j["company"]
		bank_detail = frappe.db.get_list("Bank Account",{"company":j["company"],"is_company_account":1},["bank","bank_account_no","closing_balance"])
		resp_dict["BankName"]=bank_detail[0]["bank"] if bank_detail else ""
		resp_dict["AccountNo"]=bank_detail[0]["bank_account_no"] if bank_detail else ""
		resp_dict["ClosingBalance"]=bank_detail[0]["closing_balance"] if bank_detail else ""
		l.append(resp_dict)
	return l

@frappe.whitelist()
def get_customer(start_date=None,end_date=None,name=None):
	l=[]
	filters = ""
	if start_date and end_date and name:
		filters = "where creation >= \'{0}\' and creation <= \'{1}\' and company = \'{2}\' ".format(start_date,end_date,name)
	if start_date and end_date:
		filters = "where creation >= \'{0}\' and creation <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters="where company = \"{0}\" ".format(name)
	customer_list = frappe.db.sql(''' select  company as CompanyCode ,name as CustomerId ,customer_name as CustomerName,name,mobile_no as MobileNo,email_id as EmailId,payment_terms as PaymentTerm, mobile_no as TelephoneNo,disabled ,customer_group as GroupName,
	creation as CreationDateTime,modified as ModifiedDateTime from `tabCustomer` {0} order by creation '''.format(filters),as_dict=1)
	for j in customer_list:
		j['PaymentTerm'] = frappe.db.get_value('Payment Terms Template Detail',{"parent":j['PaymentTerm']},'payment_term')
		j['ContactPersonName'] = frappe.db.get_value('')
		j["CreditLimit"]=frappe.db.get_value("Customer Credit Limit",{"company":j["CompanyCode"],"parent":j["name"]},"credit_limit") or ""
		disabled = "Y"
		if j["disabled"]:
			disabled = "N"
		j["IsActive"]=disabled
		del j['name']
	return customer_list

@frappe.whitelist()
def get_customer_transaction_receivables(start_date = None , end_date = None , name = None):
        column,data,s1,s2 = execute()
	result = []
	tot_outstand = 0
	for i in data:
		d = {}
		doc = frappe.get_doc(i[3],i[4])
		d["CompanyCode"] = doc.company
		d["TransactionId"] = i[4]
		d["CustomerId"] = i[1]
		if i[3] == "Journal Entry":
			d["CustomerName"] = frappe.db.get_value("Customer",i[1],"customer_name") if i[1] else ""
		elif i[3] == "Payment Entry":
			d["CustomerName"] = doc.party_name
		else:
			d["CustomerName"] = doc.customer_name
		d["InvoiceNo"] = i[4]
		d["InvoiceDate"] = i[0]
		d["InvoiceAmountHC"] = i[6]
		d["InvoiceAmountHC_WOT"] = i[6]
		d["InvoiceAmountFC"] = i[6]
		d["InvoiceAmountFC_WOT"] = i[6]
		d["BalanceAmountHC"] = i[9]
		d["BalanceAmountFC"] = i[9]
		d["ExchangeRate"] = doc.conversion_rate if hasattr(doc,"conversion_rate") else None
		d["CurrencyName"] = i[16]
		d["CreationDateTime"] = doc.creation
		d["ModifiedDateTime"] = doc.modified
		d["DueDate"] = i[5]
		if i[3] == "Payment Entry":
			d["VoucherType"] = "Bank Receipt"
			d["Doctype"] = "PMT"
		elif i[3] == "Journal Entry":
			d["VoucherType"] = "Journal Voucher"
			d["Doctype"] = "JRNL"
		elif i[3] == "Sales Invoice" and i[8] > 0:
			d["VoucherType"] = "Credit Note"
			d["Doctype"] = "CN"
		else:
			d["VoucherType"] = i[3]
			d["Doctype"] = "INV"
		if start_date and end_date:
			if d["InvoiceDate"] >= (datetime.strptime(start_date,"%Y-%m-%d").date()) and d["InvoiceDate"] <= (datetime.strptime(end_date,"%Y-%m-%d").date()):
				result.append(d)
			else:
				continue
		if name:
			if d["CompanyCode"] == name:
				result.append(d)
			else:
				continue
		result.append(d)
		tot_outstand += i[9]
	frappe.clear_messages()
	return result

@frappe.whitelist()
def get_customer_transactions(start_date=None,end_date=None,name=None):
	frappe.errprint("\n\n hi \n\n\n\n")                                                                                                                                                         
	filters = ""
	if start_date and end_date and name:
		filters = "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' and s.company = \"{2}\" ".format(start_date,end_date,name)
	if start_date and end_date:
		filters = "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters="and s.company = \"{0}\" ".format(name)                                                                                                                                                                                                   
	sales_invoices = frappe.db.sql('''select s.company as CompanyCode,s.name as TransactionId,s.customer as CustomerId,s.customer_name as CustomerName ,
						s.name as InvoiceNo ,s.base_grand_total as InvoiceAmountHC , s.base_net_total as InvoiceAmountHC_WOT ,s.base_grand_total as InvoiceAmountFC,
						s.base_net_total as InvoiceAmountFC_WOT ,s.outstanding_amount as BalanceAmountHC,s.outstanding_amount as BalanceAmountFC,
						s.conversion_rate as ExchangeRate,s.currency as CurrencyName,s.creation as CreationDateTime,s.modified as ModifiedDateTime,s.posting_date as pd ,s.due_date as dd
					from `tabSales Invoice` as s left join `tabCustomer` as c on c.customer_name = s.customer_name where s.docstatus=1 and s.status not in ( "Return") {0} order by s.creation'''.format(filters),as_dict= 1)
	total=0                                                                                                                                                       
	for j in sales_invoices:
		post_date = str(j["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		j["InvoiceDate"] = post_date                                                                                                                                                                
		due_date = str(j["dd"])                                                                                                                                                                       
		due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
		j["DueDate"] = due_date                                                                                                                                                                     
		j["OrderNo"] = frappe.db.get_value("Sales Invoice Item",{"parent":j["TransactionId"]},"sales_order")                                                                                                 
		j["OrderDate"] = "NA"                                                                                                                                                                       
		j["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["TransactionId"]},"sales_person")                                                                                                
		j["VoucherType"] = "Sales Invoice"                                                                                                                                                                    
		j["Doctype"] = "INV"
		del j["pd"]
		del j['dd']
		total=total+j["BalanceAmountHC"]
		frappe.errprint(str(j["TransactionId"])+": "+str(j["BalanceAmountHC"]))
	frappe.errprint("Sales inv\n")
	frappe.errprint(total)
	filters = ""
	total1 = 0
	if start_date and end_date:
		filters += "and p.posting_date >= \'{0}\' and p.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters+="and s.company = \"{0}\" ".format(name)
	payment_entries = frappe.db.sql('''select s.company as CompanyCode,p.name as TransactionId,s.customer as CustomerId,s.customer_name as CustomerName ,
						p.name as InvoiceNo ,s.base_grand_total as InvoiceAmountHC , s.base_net_total as InvoiceAmountHC_WOT ,s.base_grand_total as InvoiceAmountFC,
						s.base_net_total as InvoiceAmountFC_WOT ,pr.outstanding_amount as BalanceAmountHC,pr.outstanding_amount as BalanceAmountFC,
						s.conversion_rate as ExchangeRate,s.currency as CurrencyName,p.creation as CreationDateTime,p.modified as ModifiedDateTime,p.posting_date as pd ,s.due_date as dd
						from `tabSales Invoice` as s  inner join `tabPayment Entry Reference` as pr on pr.reference_name = s.name inner join 
						`tabPayment Entry` as p on p.name = pr.parent where s.docstatus=1 and s.outstanding_amount > 0 {0} order by p.creation'''.format(filters),as_dict= 1)
	for i in payment_entries:
		post_date = str(i["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		i["InvoiceDate"] = post_date
		due_date = str(i["dd"])                                                                                                                                                                       
		due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
		i["DueDate"] = due_date                                                                                                                                                                       
		i["VoucherType"] = "Bank Receipt"                                                                                                                                                                    
		i["Doctype"] = "PMT"
		del i["pd"]
		del i['dd']
		total1 += i["BalanceAmountFC"]
	frappe.errprint("bank receipt\n")
	frappe.errprint(total1)
	filters = ""
	total2 = 0
	if start_date and end_date:
		filters += "and j.posting_date >= \'{0}\' and j.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters+="and j.company = \"{0}\" ".format(name)
	journal_voucher = frappe.db.sql('''select distinct j.company as CompanyCode,j.name as TransactionId,je.party as CustomerId,j.title as CustomerName ,
						j.name as InvoiceNo ,j.total_amount as InvoiceAmountHC , j.total_amount as InvoiceAmountHC_WOT ,j.total_amount as InvoiceAmountFC,
						j.total_amount as InvoiceAmountFC_WOT ,j.total_amount as BalanceAmountHC,j.total_amount as BalanceAmountFC,j.total_credit as tc,je.debit as db,
						je.exchange_rate as ExchangeRate,je.account_currency as CurrencyName,j.creation as CreationDateTime,j.modified as ModifiedDateTime,j.posting_date as pd 
						from `tabJournal Entry` as j inner join `tabJournal Entry Account` as je on j.name=je.parent where j.docstatus=1 and je.party_type="Customer" and je.party is not null {0} order by j.creation'''.format(filters),as_dict= 1)
	for i in journal_voucher:
		i['BalanceAmountHC'] = i['BalanceAmountHC'] if i['db']>0 else i['BalanceAmountHC']*-1
		i['BalanceAmountFC'] = i['BalanceAmountFC'] if i['db']>0 else i['BalanceAmountFC']*-1
		post_date = str(i["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		i["InvoiceDate"] = post_date                                                                                                                                                                      
		i["VoucherType"] = "Journal Voucher"                                                                                                                                                                    
		i["Doctype"] = "JRNL"
		del i["pd"]
		del i["tc"]
		del i["db"]
		total2 += i["BalanceAmountHC"]
	frappe.errprint("journal vouc\n")
	frappe.errprint(total2)
	filters = ""
	total3 = 0
	if start_date and end_date:
		filters += "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters+="and s.company = \"{0}\" ".format(name)
	credit_notes= frappe.db.sql('''select s.company as CompanyCode,s.name as TransactionId,s.customer as CustomerId,s.customer_name as CustomerName ,
						s.name as InvoiceNo ,s.base_grand_total as InvoiceAmountHC , s.base_net_total as InvoiceAmountHC_WOT ,s.base_grand_total as InvoiceAmountFC,
						s.base_net_total as InvoiceAmountFC_WOT ,if(s.write_off_amount < 0,s.write_off_amount ,s.outstanding_amount) as BalanceAmountFC,
						if(s.write_off_amount < 0,s.write_off_amount,s.outstanding_amount ) as BalanceAmountHC,
						s.conversion_rate as ExchangeRate,s.currency as CurrencyName,s.creation as CreationDateTime,s.modified as ModifiedDateTime,s.posting_date as pd ,s.due_date as dd
						from `tabSales Invoice` as s left join `tabCustomer` as c on c.name = s.customer where s.docstatus=1 and s.status = "Return" {0} order by s.creation'''.format(filters),as_dict= 1)
	for j in credit_notes:
		post_date = str(j["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		j["InvoiceDate"] = post_date                                                                                                                                                                
		due_date = str(j["dd"])                                                                                                                                                                       
		due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
		j["DueDate"] = due_date                                                                                                                                                                     
		j["OrderNo"] = frappe.db.get_value("Sales Invoice Item",{"parent":j["TransactionId"]},"sales_order")                                                                                                 
		j["OrderDate"] = "NA"                                                                                                                                                                       
		j["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["TransactionId"]},"sales_person")                                                                                                
		j["VoucherType"] = "Credit Note"                                                                                                                                                                    
		j["Doctype"] = "CN"
		del j["pd"]
		del j['dd']
		total3 += j["BalanceAmountFC"]
	frappe.errprint("credit note\n")
	frappe.errprint(total3)
	return sales_invoices+payment_entries+journal_voucher+credit_notes

@frappe.whitelist()
def get_customer_transactions_test(start_date=None,end_date=None,name=None):
        frappe.errprint("\n\n hi \n\n\n\n")
        filters = ""
        if start_date and end_date and name:
                filters = "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' and s.company = \"{2}\" ".format(start_date,end_date,name)
        if start_date and end_date:
                filters = "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' ".format(start_date,end_date)
        if name:
                filters="and s.customer = \"{0}\" ".format(name)
        sales_invoices = frappe.db.sql('''select s.company as CompanyCode,s.name as TransactionId,s.customer as CustomerId,s.customer_name as CustomerName ,
                                                s.name as InvoiceNo ,s.base_grand_total as InvoiceAmountHC , s.base_net_total as InvoiceAmountHC_WOT ,s.base_grand_total as InvoiceAmountFC,
                                                s.base_net_total as InvoiceAmountFC_WOT ,s.outstanding_amount as BalanceAmountHC,s.outstanding_amount as BalanceAmountFC,
                                                s.conversion_rate as ExchangeRate,s.currency as CurrencyName,s.creation as CreationDateTime,s.modified as ModifiedDateTime,s.posting_date as pd ,s.due_date as dd
                                        from `tabSales Invoice` as s inner join `tabCustomer` as c on c.name = s.customer where s.docstatus=1 and s.is_return = 0 {0} order by s.posting_date'''.format(filters),as_dict= 1)
        total=0
        for j in sales_invoices:
                post_date = str(j["pd"])
                post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]
                j["InvoiceDate"] = post_date
                due_date = str(j["dd"])
                due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]
                j["DueDate"] = due_date
                j["OrderNo"] = frappe.db.get_value("Sales Invoice Item",{"parent":j["TransactionId"]},"sales_order")
                j["OrderDate"] = "NA"
                j["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["TransactionId"]},"sales_person")
                j["VoucherType"] = "Sales Invoice"
                j["Doctype"] = "INV"
                del j["pd"]
                del j['dd']
                total=total+j["BalanceAmountHC"]
                frappe.errprint(str(j["TransactionId"])+": "+str(j["BalanceAmountHC"]))
        frappe.errprint("Sales inv\n")
        frappe.errprint(total)
        filters = ""
        total1 = 0
        if start_date and end_date:
                filters += "and p.posting_date >= \'{0}\' and p.posting_date <= \'{1}\' ".format(start_date,end_date)
        if name:
                filters+="and p.party = \"{0}\" ".format(name)
        payment_entries = frappe.db.sql('''select p.company as CompanyCode,p.name as TransactionId,p.party as CustomerId,p.party_name as CustomerName ,
                                                p.name as InvoiceNo ,p.posting_date as InvoiceDate,p.paid_amount as InvoiceAmountHC , p.paid_amount as InvoiceAmountHC_WOT ,p.paid_amount as InvoiceAmountFC,
                                                p.paid_amount as InvoiceAmountFC_WOT ,if(p.unallocated_amount=0,p.unallocated_amount,p.unallocated_amount*-1) as BalanceAmountHC,if(p.unallocated_amount=0,p.unallocated_amount,p.unallocated_amount*-1) as BalanceAmountFC,p.posting_date as pd,
                                                p.source_exchange_rate as ExchangeRate ,p.paid_from_account_currency as CurrencyName,p.creation as CreationDateTime,p.modified as ModifiedDateTime
                                from `tabPayment Entry` as p where p.docstatus=1 and p.payment_type = "Receive" and p.party_type = "Customer" {0} order by p.posting_date'''.format(filters),as_dict= 1)
        for i in payment_entries:
                post_date = str(i["pd"])
                post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]
                i["InvoiceDate"] = post_date
                i["VoucherType"] = "Bank Receipt"
                i["Doctype"] = "PMT"
                del i["pd"]
                total1 += i["BalanceAmountFC"]
        frappe.errprint("bank receipt\n")
        frappe.errprint(total1)
	filters = ""
        total2 = 0
        if start_date and end_date:
                filters += "and j.posting_date >= \'{0}\' and j.posting_date <= \'{1}\' ".format(start_date,end_date)
        if name:
                filters+="and je.party = \"{0}\" ".format(name)
        journal_voucher = frappe.db.sql('''select distinct j.company as CompanyCode,j.name as TransactionId,je.party as CustomerId,j.title as CustomerName ,
                                                j.name as InvoiceNo ,j.total_amount as InvoiceAmountHC , j.total_amount as InvoiceAmountHC_WOT ,j.total_amount as InvoiceAmountFC,
                                                j.total_amount as InvoiceAmountFC_WOT ,j.total_amount as BalanceAmountHC,j.total_amount as BalanceAmountFC,j.total_credit as tc,je.debit as db,
                                                je.exchange_rate as ExchangeRate,je.account_currency as CurrencyName,j.creation as CreationDateTime,j.modified as ModifiedDateTime,j.posting_date as pd 
                                                from `tabJournal Entry` as j inner join `tabJournal Entry Account` as je on j.name=je.parent where je.reference_type = "Sales Invoice" and j.docstatus=1 and je.party_type="Customer" and je.party is not null {0} order by j.creation'''.format(filters),as_dict = 1)
        for i in journal_voucher:
                i['BalanceAmountHC'] = i['BalanceAmountHC'] if i['db']>0 else i['BalanceAmountHC']*-1
                i['BalanceAmountFC'] = i['BalanceAmountFC'] if i['db']>0 else i['BalanceAmountFC']*-1
                post_date = str(i["pd"])                                                                                                                                                                  
                post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
                i["InvoiceDate"] = post_date                                                                                                                                                                      
                i["VoucherType"] = "Journal Voucher"                                                                                                                                                                    
                i["Doctype"] = "JRNL"
                del i["pd"]
                del i["tc"]
                del i["db"]
                total2 += i["BalanceAmountHC"]
        frappe.errprint("journal vouc\n")
        frappe.errprint(total2)
        filters = ""
        total3 = 0
        if start_date and end_date:
                filters += "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' ".format(start_date,end_date)
        if name:
                filters+="and s.customer = \"{0}\" ".format(name)
        credit_notes= frappe.db.sql('''select s.company as CompanyCode,s.name as TransactionId,s.customer as CustomerId,s.customer_name as CustomerName ,
                                                s.name as InvoiceNo ,s.base_grand_total as InvoiceAmountHC , s.base_net_total as InvoiceAmountHC_WOT ,s.base_grand_total as InvoiceAmountFC,
                                                s.base_net_total as InvoiceAmountFC_WOT ,if(s.write_off_amount < 0,s.write_off_amount ,s.outstanding_amount) as BalanceAmountFC,
                                                if(s.write_off_amount < 0,s.write_off_amount,s.outstanding_amount ) as BalanceAmountHC,
                                                s.conversion_rate as ExchangeRate,s.currency as CurrencyName,s.creation as CreationDateTime,s.modified as ModifiedDateTime,s.posting_date as pd ,s.due_date as dd
                                                from `tabSales Invoice` as s left join `tabCustomer` as c on c.name = s.customer where s.docstatus=1 and s.is_return = 1 {0} order by s.creation'''.format(filters),as_dict= 1)
        for j in credit_notes:
                post_date = str(j["pd"])                                                                                                                                                                  
                post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
                j["InvoiceDate"] = post_date                                                                                                                                                                
                due_date = str(j["dd"])                                                                                                                                                                       
                due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
                j["DueDate"] = due_date                                                                                                                                                                     
                j["OrderNo"] = frappe.db.get_value("Sales Invoice Item",{"parent":j["TransactionId"]},"sales_order")                                                                                                 
                j["OrderDate"] = "NA"                                                                                                                                                                       
                j["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["TransactionId"]},"sales_person")                                                                                                
                j["VoucherType"] = "Credit Note"                                                                                                                                                                    
                j["Doctype"] = "CN"
                del j["pd"]
                del j['dd']
                total3 += j["BalanceAmountFC"]
        frappe.errprint("credit note\n")
        frappe.errprint(total3)
	frappe.errprint("Total Outstanding\n")
	frappe.errprint(total+total1+total2+total3)
        return sales_invoices+payment_entries+journal_voucher+credit_notes,(total+total1+total2+total3)

@frappe.whitelist()
def get_recadjustment(start_date=None,end_date=None,name=None,invoice_no=None):
	res=[]
	filters = ""
	if start_date:
		filters += "and p.posting_date >= \'{0}\' ".format(start_date)
	if end_date:
		filters += "and p.posting_date <= \'{0}\' ".format(end_date)
	if name:
		filters += "and p.customer = \"{0}\" ".format(name)
	if invoice_no:
		filters="and p.name = \'{0}\' ".format(invoice_no)
	sales_invoice_list = frappe.db.sql('''select si.company as CompanyCode,p.name as TransactionId,p.name as ReceiptNo,si.name as InvoiceNo,p.paid_amount as ReceiptAmountHC,p.paid_amount as ReceiptAmountFC,
	p.source_exchange_rate as ExchangeRate,p.paid_from_account_currency as CurrencyName,si.customer as CustomerId,si.customer_name as CustomerName,si.adjustment_type as AdjustmentType,
	pr.parenttype as VoucherType,p.creation as CreationDateTime,p.modified as ModifiedDateTime from `tabSales Invoice` as si  inner join `tabPayment Entry Reference` as pr
	 on pr.reference_name = si.name inner join `tabPayment Entry` as p on p.name = pr.parent where p.docstatus != 2 {0}'''.format(filters),as_dict = 1)
	return sales_invoice_list

@frappe.whitelist()
def get_pdcreceived(start_date=None,end_date=None,name = None):
    l=[]
    filters = ""
    if start_date and end_date and name:
        filters = "where posting_date >= \'{0}\' and posting_date <= \'{1}\' and company = \"{2}\" ".format(start_date,end_date,name)
    if start_date and end_date:
        filters = "where posting_date >= \'{0}\' and posting_date <= \'{1}\' ".format(start_date,end_date)
    if name:
        filters="where company = \"{0}\" ".format(name)
    sales_invoice_list = frappe.db.sql(''' select company,name,customer,customer_name,currency,conversion_rate,creation,modified from `tabSales Invoice` {0} '''.format(filters),as_dict=1)
    for j in sales_invoice_list:
        resp_dict={}
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["TransactionId"] = "NA"
        resp_dict["PDCNo"] = "NA"
        resp_dict["PDCDate"] = "NA"
        resp_dict["InvoiceNo"] = j["name"]
        resp_dict["PDCAmount"] = "NA"
        resp_dict["SalesPerson"] = frappe.db.get_value("Sales Team",{"parent":j["name"]},"sales_person")
        resp_dict["CustomerId"] = j["customer"]
        resp_dict["CustomerName"] = j["customer_name"]
        resp_dict["PDCAmountFC"] = "NA"
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        resp_dict["IsCleared"]= "Y"
        l.append(resp_dict)
    return l


@frappe.whitelist()
def get_vendor(start_date=None,end_date=None,name = None):
	l=[]
	filters = ""
	if start_date and end_date and name:
		filters = "where creation >= \'{0}\' and creation <= \'{1}\' and company = \"{2}\" ".format(start_date,end_date,name)
	if start_date and end_date:
		filters = "where creation >= \'{0}\' and creation <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters="where company = \"{0}\" ".format(name)
	supplier_list = frappe.db.sql(''' select company as CompanyCode,name as VendorId,supplier_group as GroupName,supplier_primary_contact as ContactPersonName,email_id as EmailId,
			mobile_no as MobileNo,disabled,primary_address as BillAddress,primary_address as ShipAddress,creation as CreatedDateTime,modified as ModifiedDateTime,
			supplier_name as VendorName from `tabSupplier` {0} order by creation '''.format(filters),as_dict=1)
	for i in supplier_list:
		i["FaxNo"] = "NA"
		i["CreditLimit"] = "NA"
		i["PaymentTerm"] = "NA"
		is_active="Y"
		if i["disabled"]:
			is_active = "N"
		i["IsActive"] = is_active
	return supplier_list

@frappe.whitelist()
def get_vendortransaction_payable(start_date = None , end_date = None , name = None):
        column,data,s1,s2 = execute_ap()
        result = []
        tot_outstand = 0
        for i in data:
                d = {}
                doc = frappe.get_doc(i[2],i[3])
                d["CompanyCode"] = doc.company
                d["BillNo"] = i[3]
                d["VendorId"] = i[1]
                if i[2] == "Journal Entry":
                        d["VendorName"] = frappe.db.get_value("Supplier",i[1],"supplier_name") if i[1] else ""
                elif i[2] == "Payment Entry":
                        d["VendorName"] = doc.party_name
                else:
                        d["VendorName"] = doc.supplier_name
                d["OrderNo"] = i[3]
                d["OrderDate"] = i[0]
                d["BillAmountHC"] = i[7]
                d["BillAmountHC_WOT"] = i[7]
                d["BillAmountFC"] = i[7]
                d["BillAmountFC_WOT"] = i[7]
                d["BalanceAmountHC"] = i[10]
                d["BalanceAmountFC"] = i[10]
                d["ExchangeRate"] = doc.conversion_rate if hasattr(doc,"conversion_rate") else None
                d["CurrencyName"] = i[17]
                d["CreationDateTime"] = doc.creation
                d["ModifiedDateTime"] = doc.modified
                d["DueDate"] = i[4]
                if i[2] == "Payment Entry":
                        d["VoucherType"] = "Bank Receipt"
                        d["Doctype"] = "PMT"
                elif i[2] == "Journal Entry":
                        d["VoucherType"] = "Journal Voucher"
                        d["Doctype"] = "JRNL"
                elif i[2] == "Purchase Invoice" and doc.is_return:
                        d["VoucherType"] = "Debit Note"
                        d["Doctype"] = "DN"
                else:
                        d["VoucherType"] = i[2]
                        d["Doctype"] = "BIL"
                if start_date and end_date:
                        if d["BillDate"] >= (datetime.strptime(start_date,"%Y-%m-%d").date()) and d["BillDate"] <= (datetime.strptime(end_date,"%Y-%m-%d").date()):
                                result.append(d)
                        else:
                                continue
                if name:
                        if d["CompanyCode"] == name:
                                result.append(d)
                        else:
                                continue
                result.append(d)
                tot_outstand += i[10]
	frappe.clear_messages()
        return result

@frappe.whitelist()
def get_vendortransactions(start_date=None,end_date=None,name=None):
	l=[]
	filters = ""
	if start_date and end_date and name:
		filters = "and posting_date >= \'{0}\' and posting_date <= \'{1}\' and company = \"{2}\" ".format(start_date,end_date,name)
	if start_date and end_date:
		filters = "and posting_date >= \'{0}\' and posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters="and company = \"{0}\" ".format(name)
	
	purchase_invoice_list = frappe.db.sql(''' select company as CompanyCode,name as TransactionId,supplier as VendorId,supplier_name as VendorName,
							name as BillNo,base_net_total as BillAmountHC_WOT,base_net_total as BillAmountFC_WOT,base_grand_total as BillAmountHC,base_grand_total as BillAmountHC,posting_date,due_date,
							bill_no as OrderNo,bill_date as OrderDate,outstanding_amount as BalanceAmountHC,outstanding_amount as BalanceAmountFC,currency as CurrencyName,conversion_rate as ExchangeRate,creation as CreationDateTime,
							modified as ModifiedDateTime from `tabPurchase Invoice` where docstatus= 1 and status not in ("Return") {0} order by creation '''.format(filters),as_dict=1)
	for j in purchase_invoice_list:
		post_date = str(j["posting_date"])
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]
		j["BillDate"] = post_date
		due_date = str(j["due_date"])
		due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]
		j["DueDate"] = due_date
		j["SalesPersonName"] = "NA"
		j["VoucherType"] = "Purchase Invoice"
		j["Doctype"] = "BIL"
		del j["posting_date"]
		del j["due_date"]
	filters=""
	
	if start_date and end_date:
		filters += "and p.posting_date >= \'{0}\' and p.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters += "and s.company = \"{0}\" ".format(name) 
	payment_entries = frappe.db.sql('''select s.company as CompanyCode,p.name as TransactionId,s.supplier as VendorId,s.supplier_name as VendorName ,
						p.name as BillNo ,s.base_net_total as BillAmountHC_WOT , s.base_net_total as BillAmountFC_WOT ,s.base_grand_total as BillAmountHC,
						s.base_grand_total as BillAmountHC,pr.outstanding_amount as BalanceAmountHC,pr.outstanding_amount as BalanceAmountFC,
						s.conversion_rate as ExchangeRate,s.currency as CurrencyName,p.creation as CreationDateTime,p.modified as ModifiedDateTime,p.posting_date as pd
						from `tabPurchase Invoice` as s  inner join `tabPayment Entry Reference` as pr on pr.reference_name = s.name inner join `tabPayment Entry` as p on p.name = pr.parent where s.docstatus=1 and s.outstanding_amount > 0
						 {0} order by p.creation'''.format(filters),as_dict= 1)
	for i in payment_entries:
		post_date = str(i["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		i["InvoiceDate"] = post_date                                                                                                                                        
		i["DueDate"] = ""                                                                                                                                                                      
		i["VoucherType"] = "Bank Receipt"                                                                                                                                                                    
		i["Doctype"] = "PMT"
		del i["pd"]
	filters=""
	if start_date and end_date:
		filters += "and j.posting_date >= \'{0}\' and j.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters += "and j.company = \"{0}\" ".format(name)
	journal_voucher = frappe.db.sql('''select distinct j.company as CompanyCode,j.name as TransactionId,je.party as VendorId,j.title as VendorName ,
                                                j.name as BillNo ,j.total_amount as BillAmountHC_WOT , j.total_amount as BillAmountFC_WOT ,j.total_amount as BillAmountFC,
                                                j.total_amount as BillAmountHC ,j.total_amount as BalanceAmountHC,j.total_amount as BalanceAmountFC,j.total_credit as tc,je.credit as cr,
                                                je.exchange_rate as ExchangeRate,je.account_currency as CurrencyName,j.creation as CreationDateTime,j.modified as ModifiedDateTime,j.posting_date as pd 
                                                from `tabJournal Entry` as j inner join `tabJournal Entry Account` as je on j.name = je.parent where j.docstatus=1 and je.party_type="Supplier" and je.party is not null {0} order by j.creation'''.format(filters),as_dict= 1)

	for i in journal_voucher:
		i['BalanceAmountHC'] = i['BalanceAmountHC'] if i['cr']>0 else i['BalanceAmountHC']*-1
		i['BalanceAmountFC'] = i['BalanceAmountFC'] if i['cr']>0 else i['BalanceAmountFC']*-1
		post_date = str(i["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		i["InvoiceDate"] = post_date                                                                                                                                                                      
		i["VoucherType"] = "Journal Voucher"                                                                                                                                                                    
		i["Doctype"] = "JRNL"
		del i["pd"]
		del i["tc"]
		del i["cr"]
	filters = ""
	if start_date and end_date:
		filters += "and s.posting_date >= \'{0}\' and s.posting_date <= \'{1}\' ".format(start_date,end_date)
	if name:
		filters += "and s.company = \"{0}\" ".format(name)
	debit_notes= frappe.db.sql('''select s.company as CompanyCode,s.name as TransactionId,s.supplier as VendorId,s.supplier_name as VendorName,
							s.name as BillNo,s.base_net_total as BillAmountHC_WOT,s.base_net_total as BillAmountFC_WOT,s.base_grand_total as BillAmountHC,s.base_grand_total as BillAmountHC,s.posting_date,s.due_date,
							s.bill_no as OrderNo,s.bill_date as OrderDate,s.outstanding_amount as BalanceAmountHC,s.outstanding_amount as BalanceAmountFC,s.currency as CurrencyName,s.conversion_rate as ExchangeRate,s.creation as CreationDateTime,s.posting_date as pd,s.due_date as dd,
							s.modified as ModifiedDateTime from `tabPurchase Invoice` as s left join `tabSupplier` as c on c.name = s.supplier where s.docstatus=1 and s.status = "Return" {0} order by s.creation'''.format(filters),as_dict= 1)
	for j in debit_notes:
		post_date = str(j["pd"])                                                                                                                                                                  
		post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
		j["InvoiceDate"] = post_date                                                                                                                                                                
		due_date = str(j["dd"])                                                                                                                                                                       
		due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
		j["DueDate"] = due_date                                                                                                                                                                                                                                                                                                                                          
		j["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["TransactionId"]},"sales_person")                                                                                                
		j["VoucherType"] = "Debit Note"                                                                                                                                                                    
		j["Doctype"] = "DN"
		del j["pd"]
		del j['dd']
	return purchase_invoice_list+payment_entries+journal_voucher+debit_notes

@frappe.whitelist()
def get_pdcissued(start_date=None,end_date=None,name=None):
    l=[]
    filters = ""
    if start_date and end_date and name:
        filters = "where posting_date >= \'{0}\' and posting_date <= \'{1}\' and company = \"{2}\" ".format(start_date,end_date,name)
    if start_date and end_date:
        filters = "where posting_date >= \'{0}\' and posting_date <= \'{1}\' ".format(start_date,end_date)
    if name:
        filters="where company = \"{0}\" ".format(name)
    
    purchase_invoice_list = frappe.db.sql(''' select company,name,supplier,supplier_name,currency,conversion_rate,posting_date,due_date,creation,modified from `tabPurchase Invoice` {0} '''.format(filters),as_dict=1)
    for j in purchase_invoice_list:
        resp_dict={}
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["UniqueId"] = "NA"
        resp_dict["PDCNo"] = "NA"
        resp_dict["PDCDate"] = "NA"
        resp_dict["BillNo"] = j["name"]
        resp_dict["PDCAmount"] = "NA"
        resp_dict["VendorId"] = j["supplier"]
        resp_dict["VendorName"] = j["supplier_name"]
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        resp_dict["IsCleared"] = "NA"
        resp_dict["PDCAmountFC"] = "NA"
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        l.append(resp_dict)
    return l




