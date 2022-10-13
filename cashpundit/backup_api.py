from __future__ import unicode_literals
import frappe
from frappe.model.document import Document

@frappe.whitelist()
def get_company(name=None):
    l=[]
    filters =""
    if name:
        filters = "where name = \"{0}\" ".format(name)
    resp_dict = {}
    company_list = frappe.db.sql(''' select name as company from `tabCompany` {0}'''.format(filters), as_dict=1)
    for j in company_list:
        resp_dict["CompanyName"]=j["company"]
        c = frappe.db.get_list("Company",{"name":j["company"]},["abbr","default_currency","date_of_incorporation","email","creation","modified","contact_person"])
        addr = frappe.db.get_list("Address",{"name":frappe.db.get_value("Dynamic Link",{"link_doctype":"Company","link_name":j["company"]},"parent")},["address_line1","state","country"])
        resp_dict["CompanyCode"]=c[0]["abbr"]
        resp_dict["HomeCurrency"]=c[0]["default_currency"]
        resp_dict["StartDate"]=c[0]["date_of_incorporation"]
        resp_dict["Address"]=addr[0]["address_line1"]
        resp_dict["State"]=addr[0]["state"]
        resp_dict["Country"]=addr[0]["country"]
        resp_dict["ContactPerson"]=c[0]["contact_person"]
        resp_dict["Email"]=c[0]["email"]
        resp_dict["CreatedDateTime"]=c[0]["creation"]
        resp_dict["ModifiedDateTime"]=c[0]["modified"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_bankdetail(name=None):
    l=[]
    filters=""
    if name:
        filters = "where name = \"{0}\" ".format(name)
    resp_dict = {}
    company_list = frappe.db.sql(''' select name as company from `tabCompany` {0}'''.format(filters), as_dict=1)
    for j in company_list:
        resp_dict["CompanyCode"]=j["company"]
        bank_detail = frappe.db.get_list("Bank Account",{"company":j["company"]},["bank","bank_account_no","closing_balance"])
        resp_dict["BankName"]=bank_detail[0]["bank"]
        resp_dict["AccountNo"]=bank_detail[0]["bank_account_no"]
        resp_dict["ClosingBalance"]=bank_detail[0]["closing_balance"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_customer(start_date=None,end_date=None):
    l=[]
    filters = ""
    if start_date and end_date:
        filters = "where creation >= \'{0}\' and creation <= \'{1}\' ".format(start_date,end_date)
    resp_dict = {}
    customer_list = frappe.db.sql(''' select company,name as customer from `tabCustomer` {0} '''.format(filters),as_dict=1)
    for j in customer_list:
        resp_dict["CompanyCode"]=j["company"]
        customer = frappe.db.get_list("Customer",{"company":j["company"],"customer_name":j["customer"]},["customer_id","customer_name","payment_terms","disabled","customer_group","creation","modified"])
        resp_dict["CustomerId"]=customer[0]["customer_id"]
        resp_dict["CustomerName"]=customer[0]["customer_name"]
        contact_name = frappe.db.get_value("Dynamic Link",{"link_doctype":"Customer","link_name":j["customer"],"parenttype":"Contact"},"parent")
        contact_detail = frappe.db.get_list("Contact",{"name":contact_name},["first_name","phone","mobile_no"])
        resp_dict["ContactPersonName"]=contact_detail[0]["first_name"]
        resp_dict["EmailId"]=frappe.db.get_value("Contact Email",{"parent":contact_name},"email_id")
        resp_dict["MobileNo"]=contact_detail[0]["mobile_no"]
        resp_dict["TelephoneNo"]=contact_detail[0]["phone"]
        resp_dict["CreditLimit"]=frappe.db.get_value("Customer Credit Limit",{"company":j["company"],"parent":j["customer"]},"credit_limit")
        resp_dict["PaymentTerm"]= frappe.db.get_value("Payment Terms Template Detail",{"parent":customer[0]["payment_terms"]},"credit_days")
        disabled = "Y"
        if customer[0]["disabled"]:
            disabled = "N"
        resp_dict["IsActive"]=disabled
        resp_dict["GroupName"]=customer[0]["customer_group"]
        resp_dict["CreationDateTime"]=customer[0]["creation"]
        resp_dict["ModifiedDateTime"]=customer[0]["modified"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_customer_transactions(start_date,end_date):                                                                                                                                                         
    l=[]                                                                                                                                                                                                    
    resp_dict={}                                                                                                                                                                                            
    sales_invoice_list = frappe.db.sql(''' select company,name,customer,base_net_total,base_grand_total,outstanding_amount,currency,conversion_rate,posting_date,due_date,creation,modified from `tabSales I
nvoice` where  posting_date >= %s and posting_date <= %s''',(start_date,end_date),as_dict=1)                                                                                                                
    for j in sales_invoice_list:                                                                                                                                                                            
        resp_dict["CompanyCode"] = j["company"]                                                                                                                                                             
        resp_dict["CustomerId"] = frappe.db.get_value("Customer",{"name":j["customer"]},"customer_id")                                                                                                      
        resp_dict["CustomerName"] = j["customer"]                                                                                                                                                           
        resp_dict["InvoiceNo"] = j["name"]                                                                                                                                                                  
        resp_dict["InvoiceAmountHC"] = j["base_grand_total"]                                                                                                                                                
        resp_dict["InvoiceAmountHC_WOT"] = j["base_net_total"]                                                                                                                                              
        resp_dict["InvoiceAmountFC"] = j["base_grand_total"]                                                                                                                                                
        resp_dict["InvoiceAmountFC_WOT"] = j["base_net_total"]                                                                                                                                              
        resp_dict["BalanceAmountHC"] = j["outstanding_amount"]                                                                                                                                              
        resp_dict["BalanceAmountFC"] = j["outstanding_amount"]                                                                                                                                              
        resp_dict["ExchangeRate"] = j["conversion_rate"]                                                                                                                                                    
        resp_dict["CurrencyName"] = j["currency"]                                                                                                                                                           
        post_date = str(j["posting_date"])                                                                                                                                                                  
        post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]                                                                                                                                       
        resp_dict["InvoiceDate"] = post_date                                                                                                                                                                
        due_date = str(j["due_date"])                                                                                                                                                                       
        due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]                                                                                                                                         
        resp_dict["DueDate"] = due_date                                                                                                                                                                     
        resp_dict["OrderNo"] = frappe.db.get_value("Sales Invoice Item",{"parent":j["name"]},"sales_order")                                                                                                 
        resp_dict["OrderDate"] = "NA"                                                                                                                                                                       
        resp_dict["SalesPersonName"] = frappe.db.get_value("Sales Team",{"parent":j["name"]},"sales_person")                                                                                                
        resp_dict["VoucherType"] = "NA"                                                                                                                                                                     
        resp_dict["Doctype"] = "NA"                                                                                                                                                                         
        resp_dict["CreationDateTime"] = j["creation"]                                                                                                                                                       
        resp_dict["ModifiedDateTime"] = j["modified"]                                                                                                                                                       
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_recadjustment(start_date,end_date):
    l=[]
    resp_dict={}
    sales_invoice_list = frappe.db.sql(''' select company,name,customer,currency,conversion_rate,creation,modified from `tabSales Invoice` where  posting_date >= %s and posting_date <= %s''',(start_date,$
nd_date),as_dict=1)
    for j in sales_invoice_list:
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["TransactionId"] = "NA"
        resp_dict["ReceiptNo"] = "NA"
        resp_dict["InvoiceNo"] = j["name"]
        resp_dict["ReceiptAmountHC"] = "NA"
        resp_dict["ReceiptAmountFC"] = "NA"
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        resp_dict["CustomerId"] = frappe.db.get_value("Customer",{"name":j["customer"]},"customer_id")
        resp_dict["CustomerName"] = j["customer"]
        resp_dict["AdjustmentType"] = "NA"
        resp_dict["VoucherType"] = "NA"
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_pdcreceived(start_date,end_date):
    l=[]
    resp_dict={}
    sales_invoice_list = frappe.db.sql(''' select company,name,customer,currency,conversion_rate,creation,modified from `tabSales Invoice` where  posting_date >= %s and posting_date <= %s''',(start_date,e
nd_date),as_dict=1)
    for j in sales_invoice_list:
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["TransactionId"] = "NA"
        resp_dict["PDCNo"] = "NA"
        resp_dict["PDCDate"] = "NA"
        resp_dict["InvoiceNo"] = j["name"]
        resp_dict["PDCAmount"] = "NA"
        resp_dict["SalesPerson"] = frappe.db.get_value("Sales Team",{"parent":j["name"]},"sales_person")
        resp_dict["CustomerId"] = frappe.db.get_value("Customer",{"name":j["customer"]},"customer_id")
        resp_dict["CustomerName"] = j["customer"]
        resp_dict["PDCAmountFC"] = "NA"
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        resp_dict["IsCleared"]= "Y"
        l.append(resp_dict)
    return l


@frappe.whitelist()
def get_vendor(start_date=None,end_date=None):
    l=[]
    filters = ""
    if start_date and end_date:
        filters = "where creation >= \'{0}\' and creation <= \'{1}\' ".format(start_date,end_date)
    resp_dict = {}
    supplier_list = frappe.db.sql(''' select supplier_name as supplier from `tabSupplier` {0} '''.format(filters),as_dict=1)
    for j in supplier_list:
        company_name = frappe.db.get_value("Allowed To Transact With",{"parent":j["supplier"],"parenttype":"Supplier"},"company")
        resp_dict["CompanyCode"] = company_name
        supplier_name = j["supplier"]
        supplier_details = frappe.db.get_list("Supplier",{"name":supplier_name},["supplier_name","supplier_group","supplier_primary_contact","email_id","mobile_no","disabled","primary_address","creation","modified"])
        resp_dict["VendorId"] = supplier_name
        resp_dict["VendorName"] = supplier_details[0]["supplier_name"]
        resp_dict["ContactPersonName"] = supplier_details[0]["supplier_primary_contact"]
        resp_dict["EmailId"] = supplier_details[0]["email_id"]
        resp_dict["MobileNo"] = supplier_details[0]["mobile_no"]
        resp_dict["TelephoneNo"] = supplier_details[0]["mobile_no"]
        resp_dict["FaxNo"] = "NA"
        resp_dict["CreditLimit"] = "NA"
        resp_dict["PaymentTerm"] = "NA"
        is_active="Y"
        if supplier_details[0]["disabled"]:
            is_active = "N"
        resp_dict["IsActive"] = is_active
        resp_dict["BillAddress"] = supplier_details[0]["primary_address"]
        resp_dict["ShipAddress"] = supplier_details[0]["primary_address"]
        resp_dict["GroupName"] = supplier_details[0]["supplier_group"]
        resp_dict["CreatedDateTime"] = supplier_details[0]["creation"]
        resp_dict["ModifiedDateTime"] = supplier_details[0]["modified"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_vendortransactions(start_date,end_date):
    l=[]
    resp_dict={}
    purchase_invoice_list = frappe.db.sql(''' select company,name,supplier,base_net_total,base_grand_total,bill_no,bill_date,outstanding_amount,currency,conversion_rate,posting_date,due_date,creation,modi
fied from `tabPurchase Invoice` where  posting_date >= %s and posting_date <= %s''',(start_date,end_date),as_dict=1)
    for j in purchase_invoice_list:
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["TransactionId"] = j["name"]
        resp_dict["VendorId"] = j["supplier"]
        resp_dict["VendorName"] = j["supplier"]
        resp_dict["BillNo"] = j["name"]
        resp_dict["BillAmountHC"] = j["base_grand_total"]
        resp_dict["BillAmountHC_WOT"] = j["base_net_total"]
        resp_dict["BillAmountFC"] = j["base_grand_total"]
        resp_dict["BillAmountFC_WOT"] = j["base_net_total"]
        resp_dict["BalanceAmountHC"] = j["outstanding_amount"]
        resp_dict["BalanceAmountFC"] = j["outstanding_amount"]
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        post_date = str(j["posting_date"])
        post_date= post_date[8:]+"-"+post_date[5:7]+"-"+post_date[:4]
        resp_dict["BillDate"] = post_date
        due_date = str(j["due_date"])
        due_date = due_date[8:]+"-"+ due_date[5:7]+"-"+due_date[:4]
        resp_dict["DueDate"] = due_date
        resp_dict["OrderNo"] = j["bill_no"]
        resp_dict["OrderDate"] = j["bill_date"]
        resp_dict["SalesPersonName"] = "NA"
        resp_dict["VoucherType"] = "NA"
        resp_dict["Doctype"] = "NA"
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        l.append(resp_dict)
    return l

@frappe.whitelist()
def get_pdcissued(start_date,end_date):
    l=[]
    resp_dict={}
    purchase_invoice_list = frappe.db.sql(''' select company,name,supplier,currency,conversion_rate,posting_date,due_date,creation,modified from `tabPurchase Invoice` where  posting_date >= %s and posting
_date <= %s''',(start_date,end_date),as_dict=1)
    for j in purchase_invoice_list:
        resp_dict["CompanyCode"] = j["company"]
        resp_dict["UniqueId"] = "NA"
        resp_dict["PDCNo"] = "NA"
        resp_dict["PDCDate"] = "NA"
        resp_dict["BillNo"] = j["name"]
        resp_dict["PDCAmount"] = "NA"
        resp_dict["VendorId"] = j["supplier"]
        resp_dict["VendorName"] = j["supplier"]
        resp_dict["CreationDateTime"] = j["creation"]
        resp_dict["ModifiedDateTime"] = j["modified"]
        resp_dict["IsCleared"] = "NA"
        resp_dict["PDCAmountFC"] = "NA"
        resp_dict["ExchangeRate"] = j["conversion_rate"]
        resp_dict["CurrencyName"] = j["currency"]
        l.append(resp_dict)
    return l
