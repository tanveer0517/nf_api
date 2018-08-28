import psycopg2
import json
import datetime
import decimal
from time import mktime
from flask import Response
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib,urllib2
import requests


#production database
hostname = 'wf-erp01-wr.withfloats.com'
username = 'nferp10'
password = 'NFerpV10!.'
database = 'NowFloatsV10'


#Helps to decode datatypes from postgres output
class PostgresJsonEncoder(json.JSONEncoder):

    def default(self, obj):
	if isinstance(obj, datetime.datetime):
	    try:
		return (obj.strftime('%Y-%m-%d %HH:%MM:%SS'))
	    except Exception as e:
		return str(e)

	if isinstance(obj, datetime.date):
	    try:
		return (obj.strftime('%Y-%m-%d'))
	    except Exception as e:
		return str(e)

        if isinstance(obj, decimal.Decimal):
	    try:
		return float(obj)
	    except Exception as e:
		return str(e)
#            return float(obj)

        return json.JSONEncoder.default(self, obj)

class PostgresConnector:

    def __init__(self):
	try:
            self.conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
	    self.conn.autocommit=True
	except Exception as e:
	    return str(e)

    #This function is used to check if db connection is established
    def ConnectToDatabase(self):
        try:
            self.conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
	    conn.autocommit = True
            return "OK"
        except Exception as e:
            return str(e)

#APIs using


##CHECK THE VALIDITY OF EMPLOYEE BASED ON EMAIL-ID:
    def GetUserId(self, loginKey):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT login, id FROM res_users WHERE active='t' and login=%s" % str("'"+loginKey+"'")
            cur.execute(query)

            results = []
            columns = ('login', 'userId')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)

	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET LEAD DETAILS BASED ON LEAD REFERENCE NUMBER:
    def GetLeadDetails(self, leadName):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT lead.name as leadRef,lead.c_customer_fname as customername,lead.c_customer_lname as customerlastname,lead.street as addr1,lead.street2 as addr2,city.name as city,lead.zip,lead.c_contact_mobile as mobile,lead.c_contact_email as email FROM crm_lead lead,ouc_city city WHERE lead.c_city_id = city.id and lead.active='t' and lead.name=%s" % str("'"+leadName+"'")
            cur.execute(query)

            results = []
            columns = ('leadRef','customername','customerlastname','addr1','addr2','city','zip','mobile','email')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET INVOICE DETAILS FOR SUPPORT:
    def GetAllInvoices(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "select invoice_number,fptag,sales_conversion_date,package_activation_date,sales_channel,currency_code,validity,base_price,discount_percentage,amount,status,packageid from nf_support_view"
            cur.execute(query)

            results = []
            columns = ('invoice_number','fptag','sales_conversion_date','package_activation_date','sales_channel','currency_code','validity','base_price','discount_percentage','amount','status','packageid')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET FOS HANDLE TEMP1:
    def FHTemp(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "select inv_id,user_id,emp_id,name,active,fptags,employee_email from fos_handle_temp1"
            cur.execute(query)

            results = []
            columns = ('inv_id','user_id','emp_id','name','active','fptags','employee_email')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET FOS HANDLE MANAGER:
    def FHManagers(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "select user_id,emp_id,emp_handle,reporting_head_id,manager_handle,employee_email,manager_email,chc from fos_handle_emp_manager"
            cur.execute(query)

            results = []
            columns = ('user_id','emp_id','emp_handle','reporting_head_id','manager_handle','employee_email','manager_email','chc')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET SALES PERSON EMAIL BASED ON SOURCE DOCUMENT FROM INVOICE:
    def SPEmail(self,refNumber):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            cur.execute("SELECT res.login as email from account_invoice ai,res_users res where res.id=ai.user_id and ai.origin =%s",(refNumber,))
            results = []
            columns = ('email')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET LIST OF ALL LEADS BASED ON EMAIL:
    def getLeadList(self,spEmail):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            cur.execute("SELECT lead.name as lead_number,lead.c_customer_fname as firstname,lead.c_customer_lname as lastname from crm_lead lead,res_users res where res.id=lead.user_id and res.login =%s",(spEmail,))
            results = []
            columns = ('lead_number','firstname','lastname')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#CREATE MEETING
#Work later
    def CreateMeeting(self, userlogin,leadNumber,description,meetingstatus,concernedperson,fptag,meetingType):
        try:
            if(not self.conn):
                self.ConnectToDatabase()
            cur = self.conn.cursor()
	    create_date = datetime.datetime.now()
	    try:
		query_user = "SELECT id from res_users where active='t' and login=%s" % "'"+userlogin+"'"
		cur.execute(query_user)
		user_id = cur.fetchone()[0]
	    except Exception as e:
		return 'No user existing with the given email'
	    try:
		query_lead = "SELECT id from crm_lead where name=%s" % "'"+leadNumber+"'"
		cur.execute(query_lead)
		lead_id = cur.fetchone()[0]
	    except Exception as e:
		return "Invalid Lead Number"
	    try:
		query_section = "SELECT default_section_id from res_users where active='t' and login=%s" % "'"+userlogin+"'"
		cur.execute(query_section)
		section_id = cur.fetchone()[0]
	    except Exception as e:
		return "Login is not configured. Please contact HR Team"

	    
	    try:
		cur.execute("INSERT INTO crm_opportunity2phonecall (create_uid,create_date, user_id, name, categ_id, section_id, note, date, action, nf_meeting_status,nf_contact_status,nf_fptag,nf_demo_meeting) VALUES (%s,%s,%s,%s,9,%s,%s,%s,'log',%s,%s,%s,%s)",(user_id,create_date,user_id,leadNumber,section_id,description,create_date,meetingstatus,concernedperson,fptag,meetingType,))
		cur.execute("select id,partner_id,mobile00,(select mobile from res_partner WHERE id = partner_id) AS partner_mobile FROM crm_lead WHERE name = %s",(leadNumber,))
		phone_details = cur.fetchone()
		cur.execute("INSERT INTO crm_phonecall (name,opportunity_id,user_id,categ_id,description,date,section_id,partner_id,partner_mobile,active) VALUES (%s, %s, %s, 9, %s, %s, %s,%s,%s,'t')",(leadNumber,phone_details[0],user_id,description,create_date,section_id,phone_details[1],phone_details[2],))
		return "OK"
	    except Exception as e:
		return str(e)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)



#GET HIERARCHY OF EMPLOYEE BASED ON EMAIL ID:
    def getHierarchy(self,empEmail,i=0):
	i = 0
	emp_hierarchy = ''
	try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    while i<5:
		cur.execute("select parent_id from hr_employee where work_email =%s",(empEmail,))
		emp_parent_id  = cur.fetchone()

		cur.execute("select coach_id from hr_employee where work_email =%s",(empEmail,))
		emp_rep_id  = cur.fetchone()
		if emp_parent_id:
		    cur.execute("select work_email from hr_employee where id=%s",(emp_parent_id[0],))
		    emp_parent_email = cur.fetchone()[0]
		    emp_hierarchy = emp_hierarchy + str(emp_parent_email) + ","
		if emp_rep_id and (emp_parent_id != emp_rep_id):
		    cur.execute("select work_email from hr_employee where id=%s",(emp_rep_id[0],))
		    emp_rep_email = cur.fetchone()[0]
		    emp_hierarchy = emp_hierarchy + str(emp_rep_email) + ","
		    empEmail = emp_rep_email
		else:
		    emp_hierarchy = emp_hierarchy
		    i=6
		i=i+1
            return emp_hierarchy
        except Exception as e:
            return str(e)

#GET EMPLOYEE COUNT WITH RESPECT TO CITY:
    def getEmployeeCount(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
            cur.execute("select count(emp.id),work_location as branch_name from hr_employee emp,resource_resource res where emp.resource_id=res.id and res.active='t' group by emp.work_location order by emp.work_location")
            results = []
            columns = ('count','city')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET FOS HANDLE TEMP1 BASED ON FPTAG:
    def getFPHandleList(self,FPTag):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            cur.execute("select inv_id,user_id,emp_id,name,active,fptags,employee_email from fos_handle_temp1 where fptags LIKE %s",(FPTag,))

            results = []
            columns = ('inv_id','user_id','emp_id','name','active','fptags','employee_email')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET PACKAGE EXTENSIONS:
    def GetPackageExtensions(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT reference,creation_date,nf_fptags,sale_type_status,invoice_number,c_pkg_activation_date as package_activation_date,packageid,product_validity,division,customer_name,business_name, customer_mobile,customer_email FROM nf_pkg_extention_view"
            cur.execute(query)

            results = []
            columns = ('reference','creation_date','fptag','sale_type','invoice_number','package_activation_date','packageid','validity','sales_channel','customer_segment','customername','businessname','customermobile','customeremail')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET ACTIVE EMPLOYEE AND RESPECTIVE DESIGNATIONS:
    def GetActiveEmployees(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT res.name,emp.nf_emp,emp.work_email,job.name,ofh.name, emp.c_nf_chc, div.name AS division FROM hr_employee emp,resource_resource res,hr_job job, ouc_fos_handle ofh, hr_department div where emp.resource_id=res.id and emp.intrnl_desig=job.id and emp.c_fos_handle = ofh.id and emp.sub_dep = div.id and res.active='t'"
            cur.execute(query)

            results = []
            columns = ('name', 'employeeId', 'email', 'designation', 'fosHandle', 'CFT', 'division')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


#GET SPECIFIC EMPLOYEE AND RESPECTIVE DESIGNATIONS:
    def GetSpecificEmployees(self,email):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT res.name," \
					"emp.nf_emp," \
					"emp.work_email," \
					"emp.intrnal_desig," \
					"ofh.name, " \
					"emp.c_nf_chc, " \
					"div.name AS division," \
					"branch.name AS emp_branch," \
					"(SELECT work_email FROM hr_employee WHERE id = branch.manager_id) AS BranchPOC," \
					"(SELECT work_email FROM hr_employee WHERE id = emp.coach_id) AS reporting_head," \
					"(SELECT work_email FROM hr_employee WHERE id = emp.parent_id) AS manager " \
					"FROM hr_employee emp " \
					"LEFT JOIN resource_resource res ON emp.resource_id = res.id " \
					"LEFT JOIN ouc_fos_handle ofh ON emp.c_fos_handle = ofh.id " \
					"LEFT JOIN hr_department div ON emp.sub_dep = div.id " \
					"LEFT JOIN hr_branch branch ON emp.branch_id = branch.id " \
					"WHERE res.active='t' and emp.work_email= '{}'"\
				.format(email)
            cur.execute(query)
	    emp_details = cur.fetchall()
            results = []
            columns = ('name', 'employeeId', 'email', 'designation', 'fosHandle', 'CFT', 'division', 'branch', 'BranchPOC', 'ReportingHead', 'manager')
	    results = map(lambda x: (dict(zip(columns, x))), emp_details)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET LATEST INVOICE DETAILS:
    def GetLatestInvoice(self,fptag):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select max(package_activation_date) from nf_inv_fos_handle_view where fptag=%s",(fptag,))
		act_date = cur.fetchone()[0]
		if act_date == None:
		    return "No Invoice Found for the mentioned FPTAG"
	    except:
		return "No Invoice Found for the mentioned FPTAG"

	    cur.execute("select count(*) from nf_inv_fos_handle_view where fptag=%s and package_activation_date=%s",(fptag,act_date,))
	    count_same_day_activations = cur.fetchone()[0]
	    if count_same_day_activations<1:
		return "No Invoice Found for the mentioned FPTAG"
	    else:
		cur.execute("select max(invoice_number) from nf_inv_fos_handle_view where fptag=%s and package_activation_date=%s",(fptag,act_date,))
		latest_inv_num = cur.fetchone()[0]
		
		cur.execute("select fptag,package_activation_date,invoice_number,customer_name,customer_city,customer_state,invoice_status,sp_email,sp_fos_handle,sp_reporting_head_email,sp_manager_email,sp_name,sp_branch,sp_branch_city,sp_branch_state,chc_email from nf_inv_fos_handle_view where fptag=%s and package_activation_date=%s and invoice_number=%s",(fptag,act_date,latest_inv_num,))
		inv_num = cur.fetchall()
		
		results = []
		columns = ('fptag','package_activation_date','invoice_number','customer_name','customer_city','customer_state','invoice_status','sp_email','sp_fos_handle','sp_reporting_head_email','sp_manager_email','sp_name','sp_branch','sp_branch_city','sp_branch_state','chc_email')
		for row in inv_num:
		    json_row = (dict(zip(columns, row)))
		    results.append(json_row)
		res = json.dumps(results, cls=PostgresJsonEncoder)
		return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#Update CHC
    def updateCHC(self,fptag,email):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select emp.id from hr_employee emp,resource_resource res where emp.resource_id=res.id and emp.work_email=%s and res.active='t'",(email,))
		emp_id = cur.fetchone()[0]
	    except:
		return "No Employee active with the given email id"
	    try:
		cur.execute("select id from account_invoice where number=%s and type='out_invoice'",(invNumber,))
		inv_id = cur.fetchone()[0]
	    except:
		return "The given invoice number is in valid"
	    try:
#query to get the id of the fptag from fptag table
		cur.execute("select id from ouc_fptag where name=%s",(fptag,))
		fptag_id = cur.fetchone()[0]
		if fptag_id:
		    cur.execute("update account_invoice set nf_chc=%s from account_invoice_line ail where account_invoice.id=ail.invoice_id and ail.c_fptags_id=%s and account_invoice.state='paid' and account_invoice.type='out_invoice'",(emp_id,fptag_id,))
		    conn.commit()
		    return "Success"
		else:
		    return "FPTAG is not present in ERP"
	    except:
		return "Update failed"
        except Exception as e:
            return str(e)


#GET PROFORMA INVOICE NUMBERS FOR A SALES PERSON
    def getPIList(self,email):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select id from res_users where active='t' and login=%s",(email,))
		user_id = cur.fetchone()[0]
		if user_id == None:
		    return "No user existing with the given email id"
	    except:
		return "No user existing with the given email id"

	    try:
		cur.execute("select em.id from hr_employee em,resource_resource re where re.id=em.resource_id and re.active='t' and re.user_id=%s",(user_id,))
		emp_id = cur.fetchone()[0]
		if emp_id == None:
		    return "The user is not mapped to any active Employee. Please contact HR Team"
	    except:
		return "The user is not mapped to any active Employee. Please contact HR Team"

	    cur.execute("select so.name as pi_number from sale_order so,res_partner rp where so.partner_id=rp.id and so.state in ('draft','sent','accept') and ((so.user_id=%s and so.c_sales_support_id is NULL) or (so.c_sales_support_id=%s))",(user_id,emp_id,))
	    pi_numbers = cur.fetchall()
	    cur.execute("select ld.name as lead_name from nf_custom_quotation cq,res_partner rp,crm_lead ld where cq.partner_id=rp.id and cq.opportunity_id=ld.id and ld.claim_id is NULL and cq.sales_person_id=%s",(user_id,))
	    lead_name = cur.fetchall()
	    results = []
            quotations = pi_numbers+lead_name
#	    columns = ('pi_number')
	    for row in quotations:
		row=row[0]
		columns = row.upper()
		json_row = (columns, row)
		results.append(json_row)
	    res = json.dumps(dict(results), cls=PostgresJsonEncoder)
	    return  Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET PROFORMA INVOICE CUSTOMER LIST FOR A SALES PERSON
    def getPICustomerList(self,email):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select id from res_users where active='t' and login=%s",(email,))
		user_id = cur.fetchone()[0]
		if user_id == None:
		    return "No user existing with the given email id"
	    except:
		return "No user existing with the given email id"

	    try:
		cur.execute("select em.id from hr_employee em,resource_resource re where re.id=em.resource_id and re.active='t' and re.user_id=%s",(user_id,))
		emp_id = cur.fetchone()[0]
		if emp_id == None:
		    return "The user is not mapped to any active Employee. Please contact HR Team"
	    except:
		return "The user is not mapped to any active Employee. Please contact HR Team"

	    cur.execute("select rp.name as customername from sale_order so,res_partner rp where so.partner_id=rp.id and so.state in ('draft','sent') and ((so.user_id=%s and so.c_sales_support_id is NULL) or (so.c_sales_support_id=%s))",(user_id,emp_id,))
	    pi_numbers = cur.fetchall()
		
	    results = []
#	    columns = ('customername')
	    for row in pi_numbers:
		row=row[0]
		columns = row.upper()
		json_row = (columns, row)
		results.append(json_row)
	    res = json.dumps(dict(results), cls=PostgresJsonEncoder)
	    return  Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET Proforma Invoice Numbers List FOR A SALES PERSON and Customer Name
    def getPIListForCustomer(self,email,customerName):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select id from res_users where active='t' and login=%s",(email,))
		user_id = cur.fetchone()[0]
		if user_id == None:
		    return "No user existing with the given email id"
	    except:
		return "No user existing with the given email id"

	    try:
		cur.execute("select em.id from hr_employee em,resource_resource re where re.id=em.resource_id and re.active='t' and re.user_id=%s",(user_id,))
		emp_id = cur.fetchone()[0]
		if emp_id == None:
		    return "The user is not mapped to any active Employee. Please contact HR Team"
	    except:
		return "The user is not mapped to any active Employee. Please contact HR Team"

	    cur.execute("select so.name as piNumber from sale_order so,res_partner rp where so.partner_id=rp.id and so.state in ('draft','sent') and ((so.user_id=%s and so.c_sales_support_id is NULL) or (so.c_sales_support_id=%s)) and rp.name=%s",(user_id,emp_id,customerName,))
	    pi_numbers = cur.fetchall()
		
	    results = []
#	    columns = ('PiNumber')
	    for row in pi_numbers:
		row=row[0]
		columns = row.upper()
		json_row = (columns, row)
		results.append(json_row)
	    res = json.dumps(dict(results), cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


#GET PROFORMA INVOICE Detials BASED ON PROFORMA INVOICE NUMBER
    def getPIDetails(self,refNumber):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
            try:
                if 'NFL' in refNumber:
                    cur.execute("select COUNT(*) from nf_custom_quotation cq,crm_lead ld where cq.opportunity_id=ld.id and ld.name=%s",(refNumber,))
                else:
                    cur.execute("select COUNT(*) from sale_order where name=%s and state in ('draft','sent','accept')",(refNumber,))
                number_confirmation = cur.fetchone()[0]
                if number_confirmation == 0:
                    return "INVALID REFERENCE NUMBER"
            except:
                return "INVALID REFERENCE NUMBER"
            pi_details=[]
            type_of_sale='New'
            so_state='Draft'
            if 'NFL' in refNumber:
                cur.execute("select count(*) from sale_subscription ss,nf_custom_quotation cq,crm_lead ld where cq.partner_id=ss.partner_id and cq.opportunity_id=ld.id and ld.name=%s",(refNumber,))
                number_subscription=cur.fetchone()[0]
                if number_subscription >= 1:
                    type_of_sale='Renewal/Upsell'

                cur.execute("""SELECT """ \
                            """ld.name AS lead_name,""" \
                            """emp.work_email AS sp_email,""" \
                            """city.name AS sp_city,""" \
                            """division.name AS sp_division,""" \
                            """emp.name_related AS sp_name,""" \
                            """customer.name AS business_name,""" \
                            """(SELECT fp.name FROM ouc_fptag fp WHERE fp.lead_id = ld.id LIMIT 1) AS fptags,""" \
                            """prod_tmp.name AS product,""" \
                            """cql.quantity AS qty,""" \
                            """cql.discount,""" \
                            """cql.price_unit AS unitprice,""" \
                            """cql.id AS sol_id,""" \
                            """prod_tmp.c_package_id AS package_id,""" \
                            """cql.price_tax,""" \
                            """cql.price_subtotal AS price_untax,""" \
                            """cql.price_total,""" \
                            """( SELECT COALESCE(sum(tax.amount), 0::numeric) AS "coalesce" FROM account_tax tax """ \
                            """JOIN account_tax_nf_custom_quotation_line_rel tsol ON tsol.account_tax_id = tax.id """ \
                            """WHERE tsol.nf_custom_quotation_line_id = cql.id) AS tax_percentage """ \
                            """FROM nf_custom_quotation_line cql """ \
                            """LEFT JOIN nf_custom_quotation cq ON cql.quotation_id = cq.id """ \
                            """LEFT JOIN crm_lead ld ON cq.opportunity_id = ld.id """ \
                            """LEFT JOIN product_product prod ON cql.product_id = prod.id """ \
                            """LEFT JOIN product_template prod_tmp ON prod.product_tmpl_id = prod_tmp.id """ \
                            """LEFT JOIN resource_resource res ON cq.sales_person_id = res.user_id """ \
                            """LEFT JOIN hr_employee emp ON emp.resource_id = res.id """ \
                            """LEFT JOIN hr_branch branch ON emp.branch_id = branch.id """ \
                            """LEFT JOIN ouc_city city ON branch.c_city_id = city.id """ \
                            """LEFT JOIN hr_department division ON emp.sub_dep = division.id """ \
                            """LEFT JOIN res_partner customer ON cq.partner_id = customer.id """ \
                            """WHERE ld.name = %s """ \
                            """ORDER BY ld.name DESC""",(refNumber,))
            else:
                cur.execute("select proforma_inv_number,sp_email,sp_city,sp_division,sp_name,business_name,fptags,product,qty,discount,unitprice,sol_id,package_id,price_tax,price_untax,price_total, tax_percentage,type_of_sale,so_state  from nf_so_view where proforma_inv_number=%s",(refNumber,))
            pi_details = cur.fetchall()
            if pi_details:
                results = []
                columns = ('proforma_inv_number','sp_email','sp_city','sp_division','sp_name','business_name','fptags','product','qty','discount','unitprice','sol_id','package_id','price_tax','price_untax','price_total','tax_percentage','type_of_sale','so_state')
                for row in pi_details:
                    if 'NFL' in refNumber:
                        row=row+(type_of_sale,so_state)
                    json_row = (dict(zip(columns, row)))
                    results.append(json_row)
                res = json.dumps(results, cls=PostgresJsonEncoder)
                return Response(res, content_type='application/json; charset=utf-8')
            else:
                return "NO PRODUCTS ADDED IN THE PROFORMA INVOICE"
        except Exception as e:
            return str(e)

#GET Previous Sale Details
    def getPreviousSale(self,fptag,packageid):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    #cur.execute("select id from ouc_fptag where name=%s",(fptag,))
	    #fptag_id = cur.fetchone()[0]
	    try:
		cur.execute("select ai.origin as saleorder,ail.price_subtotal as price,ai.date_invoice as inv_date from account_invoice ai,account_invoice_line ail,product_product pr,ouc_fptag fp where ai.id=ail.invoice_id and pr.id=ail.product_id and ail.c_fptags_id = fp.id and fp.name = %s and ai.state='paid' order by ai.date_invoice desc",(fptag,))
		previous_details = cur.fetchall()
		if previous_details:
		    previous_details=previous_details[0]
		    results = []
		    columns = ('saleorder','price','date')
		    results = (dict(zip(columns, previous_details)))
		    res = json.dumps(results, cls=PostgresJsonEncoder)
		    return Response(res, content_type='application/json; charset=utf-8')
		else:
		    return "NO PRODUCTS ADDED IN THE PROFORMA INVOICE"
	    except Exception as e:
		return str(e)
        except Exception as e:
            return str(e)

#Update Discount
#Change it
    def updateDiscount(self,lineid,discount,approvalstatus,coupon_code,coupon_validity):
	discount = float(discount)
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select order_id from sale_order_line where id=%s",(lineid,))
		order_id=cur.fetchone()[0]
		cur.execute("select price_unit from sale_order_line where id=%s",(lineid,))
		price_unit=cur.fetchone()[0]
		cur.execute("select product_uom_qty from sale_order_line where id=%s",(lineid,))
		quantity=cur.fetchone()[0]
	    except:
		return "Sale Order Not Found"

	    try:
		cur.execute("select id,name from sale_order where id = %s and state IN ('sale', 'done')",(order_id,))
		so_id = cur.fetchone()
		if so_id:
		   cur.execute("UPDATE sale_order SET disc_approval_status='Discount updation failed! Sales Order already confirmed' WHERE id = %s",(order_id,))
		   conn.commit()
		   results =  "Sales Order ref {} is already confirmed! Discount cannot be updated on confirmed Sales Order".format(so_id[1])
		   res = json.dumps(results, cls=PostgresJsonEncoder)
	    	   return Response(res, content_type='application/json; charset=utf-8')
	    except:
		return "Sale Ordersss Not Found"

	#update order line
	    try:
		price_subtotal = float(price_unit)*float(quantity)*(1-discount/100)
		nf_tax_amount = float(price_subtotal)*0.18
		nf_price_subtotal_with_tax = float(nf_tax_amount) + float(price_subtotal)
		cur.execute("update sale_order_line set discount=%s,price_tax=%s,c_price_sub_with_tax=%s,price_subtotal=%s,price_total=%s where id=%s",(discount,nf_tax_amount,nf_price_subtotal_with_tax, price_subtotal,nf_price_subtotal_with_tax,lineid,))
		conn.commit()
	    except:
		return "problem updating the sale order line"

	#update sale order
	    try:
		cur.execute("select sum(round(product_uom_qty*(sol.price_unit-(sol.price_unit*sol.discount/100)),2)) from sale_order_line sol where sol.id!=%s and sol.order_id=%s",(lineid,order_id,))
		other_subtotal = cur.fetchone()
		if other_subtotal:
		    other_subtotal= other_subtotal[0]
		else:
		    other_subtotal = 0.00
		if other_subtotal:
		    other_subtotal= other_subtotal
		else:
		    other_subtotal = 0.00
		amount_untaxed = float(price_subtotal) + float(other_subtotal)
		amount_tax = float(amount_untaxed)*0.18
		amount_total = float(amount_tax) + float(amount_untaxed)
		cur.execute("update sale_order set amount_untaxed=%s,amount_tax=%s,amount_total=%s,disc_approval_status=%s,c_discount_coupon_code=%s,c_dicsount_coupon_validity=%s where id=%s",(amount_untaxed,amount_tax,amount_total,approvalstatus,coupon_code,coupon_validity/24,order_id,))
		conn.commit()
		results = "Success"
		res = json.dumps(results, cls=PostgresJsonEncoder)
	        return Response(res, content_type='application/json; charset=utf-8')
	    except:
		return "problem updating the sale order"
        except Exception as e:
            return str(e)


#Update SingleStore Discount
    def updateSingleStoreDiscount(self,lineid,discount,approvalstatus,coupon_code,coupon_validity):
        discount = float(discount)
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
            try:
                cur.execute("select quotation_id from nf_custom_quotation_line where id=%s",(lineid,))
                quotation_id=cur.fetchone()[0]
                cur.execute("select price_unit from nf_custom_quotation_line where id=%s",(lineid,))
                price_unit=cur.fetchone()[0]
                cur.execute("select quantity from nf_custom_quotation_line where id=%s",(lineid,))
                quantity=cur.fetchone()[0]
            except:
                return "Quotation Not Found"

            try:
                cur.execute("select cq.id,cq.name from nf_custom_quotation cq, crm_lead ld where cq.id = %s and cq.opportunity_id=ld.id and ld.claim_id is not NULL",(quotation_id,))
                so_id = cur.fetchone()
                if so_id:
                    cur.execute("UPDATE nf_custom_quotation SET disc_approval_status='Discount updation failed! Sales Order already claimed' WHERE id = %s",(quotation_id,))
                    conn.commit()
                    results =  "Quotation ref {} is already claimed! Discount cannot be updated on claimed Sales Order".format(so_id[1])
                    res = json.dumps(results, cls=PostgresJsonEncoder)
                    return Response(res, content_type='application/json; charset=utf-8')
            except:
                return "Quotation Not Found"

            #update order line
            try:
                price_subtotal = float(price_unit)*float(quantity)*(1-discount/100)
                nf_tax_amount = float(price_subtotal)*0.18
                nf_price_subtotal_with_tax = float(nf_tax_amount) + float(price_subtotal)
                cur.execute("update nf_custom_quotation_line set discount=%s,price_tax=%s,price_subtotal=%s,price_total=%s where id=%s",(discount,nf_tax_amount,price_subtotal,nf_price_subtotal_with_tax,lineid,))
                conn.commit()
            except:
                return "problem updating the quotation line"

            #update sale order
            try:
                cur.execute("select sum(quantity*(sol.price_unit-(sol.price_unit*sol.discount/100))) from nf_custom_quotation_line sol where sol.id!=%s and sol.quotation_id=%s",(lineid,quotation_id,))
                other_subtotal = cur.fetchone()
                if other_subtotal:
                    other_subtotal= other_subtotal[0]
                else:
                    other_subtotal = 0.00
                if other_subtotal:
                    other_subtotal= other_subtotal
                else:
                    other_subtotal = 0.00
                amount_untaxed = float(price_subtotal) + float(other_subtotal)
                amount_tax = float(amount_untaxed)*0.18
                amount_total = float(amount_tax) + float(amount_untaxed)
                cur.execute("update nf_custom_quotation set amount_untaxed=%s,amount_tax=%s,amount_total=%s,disc_approval_status=%s,discount_coupon_code=%s,dicsount_coupon_validity=%s where id=%s",(amount_untaxed,amount_tax,amount_total,approvalstatus,coupon_code,coupon_validity/24,quotation_id,))
                conn.commit()
                results = "Success"
                res = json.dumps(results, cls=PostgresJsonEncoder)
                return Response(res, content_type='application/json; charset=utf-8')
            except:
                return "problem updating the quotation"
        except Exception as e:
            return str(e)	  


#GET INVOICE FOR WHICH CHC IS NOT ASSIGNED:
    def getnoCHCDetails(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    cur.execute("select fptag,package_activation_date,invoice_number,customer_name,customer_city,customer_state,invoice_status,sp_email,sp_fos_handle,sp_reporting_head_email,sp_manager_email,sp_name,sp_branch,sp_branch_city,sp_branch_state,chc_email from nf_inv_fos_handle_view where chc_email is NULL")
	    inv_num = cur.fetchall()
		
	    results = []
	    columns = ('fptag','package_activation_date','invoice_number','customer_name','customer_city','customer_state','invoice_status','sp_email','sp_fos_handle','sp_reporting_head_email','sp_manager_email','sp_name','sp_branch','sp_branch_city','sp_branch_state','chc_email')
	    for row in inv_num:
		json_row = (dict(zip(columns, row)))
		results.append(json_row)
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


#GET GET LEAD NUMBER BASED ON CUSTOMER EMAIL AND SALES PERSON EMAIL:
    def getLeadNumberforCHC(self,customeremail,spemail):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select id from res_users where login=%s and active='t'",(spemail,))
		sp_id = cur.fetchone()[0]
	    except:
		return "CHC OR sALES PERSON EMAIL ID DOESNOT EXISTS"
	    try:
		cur.execute("select name as leadnumber from crm_lead where c_contact_email =%s and user_id=%s and c_state not in ('done','cancel')",(customeremail,sp_id,))
		lead_num = cur.fetchone()[0]
	    except:
		return "NO LEAD EXISTING"
		
	    results = []
	    columns = ('leadnumber')

	    res = json.dumps(lead_num, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


#GET LATEST INVOICE DETAILS with list of FPTAGs:
    def listOfLatestInvoice(self,fptags):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    i=0
	    results = []
	    columns = ('fptag','package_activation_date','invoice_number','customer_name','customer_city','customer_state','invoice_status','sp_email','sp_fos_handle','sp_reporting_head_email', 'sp_manager_email','sp_name','sp_branch','sp_branch_city','sp_branch_state','chc_email')
	    while i<len(fptags):
		fptag = fptags[i]
		cur.execute("select count(*) from nf_inv_fos_handle_view where fptag=%s",(fptag,))
		count_same_day_activations = cur.fetchone()[0]
		if count_same_day_activations>=1:
		    cur.execute("select max(invoice_number) from nf_inv_fos_handle_view where fptag=%s",(fptag,))
		    latest_inv_num = cur.fetchone()[0]
		
		    cur.execute("select fptag,package_activation_date,invoice_number,customer_name,customer_city,customer_state,invoice_status,sp_email,sp_fos_handle,sp_reporting_head_email,sp_manager_email,sp_name,sp_branch,sp_branch_city,sp_branch_state,chc_email from nf_inv_fos_handle_view where fptag=%s and invoice_number=%s",(fptag,latest_inv_num,))
		    inv_det = cur.fetchall()
		    for row in inv_det:
			json_row = (dict(zip(columns, row)))
			results.append(json_row)
		i=i+1

	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def send_sp_email(self, emp_email, employee_name, lead_no, business_name, customer_mobile, customer_email, customer_name, fptag):
		
	   mail_subject = "Urgent - %s has received an enquiry - Call them ASAP" % (fptag)

           html = """<!DOCTYPE html>
	                    <html>

	                        <body>
				   <p style="color:#4E0879">Hi """+str(employee_name)+""",</p>
				   <p>Your Customer """+str(fptag)+""" has received an enquiry/Potential call on their business website. Please contact him/her ASAP.</p>

			<p>Following are details :</p>

			<table style="width:100%">
	                          <tr style="width:100%">
	                            <td style="width:15%"><b>Lead</b></td>
	                            <td style="width:55%">: <span>"""+str(lead_no)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>

				  <tr style="width:100%">
	                            <td style="width:15%"><b>Customer Name</b></td>
	                            <td style="width:55%">: <span>"""+str(customer_name)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>

	                          <tr style="width:100%">
	                            <td style="width:15%"><b>Business Name</b></td>
	                            <td style="width:55%">: <span>"""+str(business_name)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>
				  
				  <tr style="width:100%">
	                            <td style="width:15%"><b>Customer Mobile</b></td>
	                            <td style="width:55%">: <span>"""+str(customer_mobile)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>

				 <tr style="width:100%">
	                            <td style="width:15%"><b>customer Email</b></td>
	                            <td style="width:55%">: <span>"""+str(customer_email)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>	
				
				 <tr style="width:100%">
	                            <td style="width:15%"><b>FP Tag</b></td>
	                            <td style="width:55%">: <span>"""+str(fptag)+"""</span></td>
				    <td style="width:30%"></td>
	                          </tr>	
			</table>

				
	                <div>
	                    <p></p>
	                </div>
                     <p>Best Regards,<br/>
		       RIA</p>
	        <html>"""
		   
	

	   email_id = [emp_email]
	   cc_id = ['mohit.katiyar@nowfloats.com','ria.nf@nowfloats.com']
	   msg = MIMEMultipart('alternative')
	   text = "plaintext"
	   part1 = MIMEText(text, 'plain')
	   html = html
	   part2 = MIMEText(html, 'html')
	   url = 'https://api.withfloats.com/Internal/v1/PushEmailToQueue/A91B82DE3E93446A8141A52F288F69EFA1B09B1D13BB4E55BE743AB547B3489E'
	   vals = {"ClientId": "A91B82DE3E93446A8141A52F288F69EFA1B09B1D13BB4E55BE743AB547B3489E", "EmailBody": html,
			"ReplyTo": "hello@nowfloats.com", "Subject": mail_subject, "To": email_id, "Type": 0, "CC": cc_id}
	   req = urllib2.Request(url)
	   req.add_header('Content-Type', 'application/json')
	   data = json.dumps(vals)
	   response = urllib2.urlopen(req, data)
	   encoder = response.read().decode('utf-8')
	   return True

    def send_ria_email(self, fptag):
		
	   mail_subject = "Urgent - %s has received an enquiry - Call them ASAP" % (fptag)

           html = """<!DOCTYPE html>
	                    <html>

	                        <body>           
				   <p><b>"""+str(fptag)+"""</b> has received an enquiry/Potential call on their business website. Please call them ASAP.</p></br>

				
	                <div>
	                    <p></p>
	                </div>
                     <p>Best Regards,<br/>
		       RIA</p>
	        <html>"""
		   
	

	   email_id = ['ria.nf@nowfloats.com']
	   cc_id = ['mohit.katiyar@nowfloats.com']
	   msg = MIMEMultipart('alternative')
	   text = "plaintext"
	   part1 = MIMEText(text, 'plain')
	   html = html
	   part2 = MIMEText(html, 'html')
	   url = 'https://api.withfloats.com/Internal/v1/PushEmailToQueue/A91B82DE3E93446A8141A52F288F69EFA1B09B1D13BB4E55BE743AB547B3489E'
	   vals = {"ClientId": "A91B82DE3E93446A8141A52F288F69EFA1B09B1D13BB4E55BE743AB547B3489E", "EmailBody": html,
			"ReplyTo": "hello@nowfloats.com", "Subject": mail_subject, "To": email_id, "Type": 0, "CC": cc_id}
	   req = urllib2.Request(url)
	   req.add_header('Content-Type', 'application/json')
	   data = json.dumps(vals)
	   response = urllib2.urlopen(req, data)
	   encoder = response.read().decode('utf-8')
	   return True	

    def send_lead_sms(self, emp_mobile, employee_name, lead_no, business_name, customer_mobile, customer_email, fptag):
	if emp_mobile:
		url = "https://api.withfloats.com/Discover/v1/floatingpoint/CreateSMS"
		mobile_no = emp_mobile
		message = "Hi, \n"+str(fptag)+" with Lead Ref "+str(lead_no)+" has received an enquiry/potential call , please call them  on "+str(customer_mobile)+" ASAP."
	
		querystring = {"mobileNumber":emp_mobile,"message":message}
		data = json.dumps("A91B82DE3E93446A8141A52F288F69EFA1B09B1D13BB4E55BE743AB547B3489E")
		
		headers = {
			    'content-type': "application/json",
			    }
		response = requests.request("POST", url, data=data, headers=headers, params=querystring)
    	        return True	  


#send_enquiry_email

    def send_roi_enquiry_email(self,fptag):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
        except Exception as e:
            return str(e)

        try:
  	  cur.execute("SELECT COALESCE(emp.work_email,'') AS emp_email, emp.mobile_phone, lead.name, lead.name001 AS business_name, lead.mobile00 AS cust_mobile, lead.email00 AS cust_email, emp.name_related AS employee_name, lead.name00 AS customer_name FROM crm_lead lead LEFT JOIN resource_resource res ON res.user_id = lead.user_id LEFT  JOIN hr_employee emp ON emp.resource_id = res.id WHERE res.active = True AND lead.fax00 = %s",(fptag,))
	  
 	  temp = cur.fetchone()
	  if temp:
	     emp_email = temp[0]
	     emp_mobile = temp[1]
	     lead_no = temp[2]
	     business_name = temp[3]
	     customer_mobile = temp[4]
	     customer_email = temp[5]	
	     employee_name = temp[6]	
	     customer_name = temp[7]	

	     self.send_sp_email(emp_email, employee_name, lead_no, business_name, customer_mobile, customer_email, customer_name, fptag) 
	     self.send_lead_sms(emp_mobile, employee_name ,lead_no, business_name, customer_mobile, customer_email, fptag)   
	  else:
	      self.send_ria_email(fptag) 
	  conn.close()		
	  return "Success"
        except Exception as e:
	    conn.close()   
            return str(e)


   #GetThdAchievement
    def ThdAchievement(self, email_id):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)

	try:
	   cur.execute("SELECT id FROM res_users WHERE login = %s",(email_id,))
	   temp = cur.fetchone()[0]
	except:
	   conn.close()
	   return "No email id OR email id is invalid"
	
	try:
	   cur.execute("select (to_char(month,'month')||'-'||date_part('year',month)) AS thd_month,ytd,achievement,crr_frwd,date_part('month',month) AS month_num FROM thd_achievement_view WHERE email_id = %s AND month <= now() at time zone 'utc' ORDER BY month_num DESC",(email_id,))
	   thd_data = cur.fetchall()
	   columns = ('month','threshold','achievement','carry_forward')
	   results = map(lambda x:(dict(zip(columns, x))),thd_data)
	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            return str(e)


#GetMeetingData
    def MeetingData(self, email_id):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)

	try:
	   cur.execute("SELECT id FROM res_users WHERE login = %s",(email_id,))
	   temp = cur.fetchone()[0]
	except:
	   conn.close()
	   return "No email id OR email id is invalid"
	
	try:
	   cur.execute("select (to_char(date_of_meeting,'month')||'-'||date_part('year',date_of_meeting)) AS month,meeting_type,SUM(number_of_meeting),date_part('month',date_of_meeting) AS month_num FROM crm_meeting_view WHERE emp_email = %s AND date_of_meeting::date > (now() at time zone 'utc' - INTERVAL '3 month')::date GROUP BY month,meeting_type,month_num ORDER BY month_num DESC",(email_id,))
	   meeting_data = cur.fetchall()
	   columns = ('month','meeting_type','number_of_meeting')
	   results = map(lambda x:(dict(zip(columns, x))),meeting_data)
	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            return str(e)


#GetFosRank
    def FosRank(self, email_id):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)

	try:
	   if email_id:
	      cur.execute("SELECT id FROM res_users WHERE login = %s",(email_id,))
	      temp = cur.fetchone()[0]
	except:
	   conn.close()
	   return "No email id OR email id is invalid"
	
	try:
	   if email_id:
		   cur.execute("select c_sp_emp_id, emp_name, work_email, branch, amt, branch_rank, national_rank FROM nf_rank_by_revenue WHERE work_email = '{}'".format(email_id))
		   emp_rank_revenue = cur.fetchall()
		   
		   columns = ('empID', 'employee', 'email', 'branch', 'revenue', 'branchRank', 'nationalRank')
		   results = map(lambda x:(dict(zip(columns, x))), emp_rank_revenue)
	   else:
		   cur.execute("select c_sp_emp_id, emp_name, work_email, branch, amt, national_rank FROM nf_rank_by_revenue WHERE work_email is NOT NULL ORDER BY national_rank LIMIT 10")
		   top_eight_rank = cur.fetchall()
		   
		   columns = ('empID', 'employee', 'email', 'branch', 'revenue', 'nationalRank')
		   results = map(lambda x:(dict(zip(columns, x))), top_eight_rank)

	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            results = {
		      'status':'Not Eligible'
		      }
	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    conn.close()
	    return Response(res, content_type='application/json; charset=utf-8')

#ExotelIncomingCall
    def ExotelIncomingCall(self, CallFrom, CallTo):
	try:
	   results = "8588096002"
	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
            results = "9756285636"
	    return results

#DincharyaPerformance
    def DincharyaPerformance(self, email_id):
         try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	 except Exception as e:
            return str(e)

	 try:
	    cur.execute("SELECT id FROM res_users WHERE login = %s",(email_id,))
	    temp = cur.fetchone()[0]
	 except:
	    conn.close()
	    return "No email id OR email id is invalid"
	
	 try:   
	     cur.execute("SELECT biz.user_id FROM nf_biz biz INNER JOIN res_users usr ON biz.user_id = usr.id WHERE usr.login = %s",(email_id,))
	     dbm = cur.fetchone()
	     if dbm:
		   FosDesig = ('Associate - FOS','Consultant - FOS','Principal Consultant - FOS','Senior Consultant - FOS','Associate Product Specialist','Principal Product Specialist','Product Specialist','Senior Product Specialist','Associate - Customer First','Consultant - Customer First','Principal Consultant - Customer First','Senior Consultant - Customer First','Associate - Verticals','Consultant - Verticals','Principal Consultant - Verticals','Senior Consultant - Verticals')
		   str_sql = "SELECT " \
					   "nb.date," \
					   "emp.name_related," \
					   "emp.nf_emp AS emp_id," \
					   "(SELECT name FROM hr_branch WHERE id = emp.branch_id) AS branch," \
					   "emp.work_email," \
					   "COUNT(nbl.id) AS team_strength ," \
					   "COUNT(CASE WHEN nbl.performance = 'GOOD' THEN 1 END) AS cn_good, " \
					   "COUNT(CASE WHEN nbl.performance = 'AVERAGE' THEN 1 END) AS cn_avg, " \
					   "COUNT(CASE WHEN nbl.performance = 'BAD' THEN 1 END) AS cn_bad, " \
					   "COUNT(CASE WHEN nbl.performance = 'ABSENT' THEN 1 END) AS cn_absent, " \
					   "CASE WHEN EXTRACT(DOW FROM nb.date - INTERVAL '1 day') = 0 THEN " \
					           "SUM(CASE WHEN nbl.meeting_date = nb.date - INTERVAL '2 day' THEN (nbl.new_meeting_num + nbl.followup_meeting_num) ELSE 0 END) " \
					         "ELSE " \
					            "SUM(CASE WHEN nbl.meeting_date = nb.date - INTERVAL '1 day' THEN (nbl.new_meeting_num + nbl.followup_meeting_num) ELSE 0 END)  " \
					    "END AS total_meeting, " \
					    "CASE WHEN EXTRACT(DOW FROM nb.date - INTERVAL '1 day') = 0 THEN " \
					 	       "(nb.date - INTERVAL '2 day')::date " \
					 	     "ELSE " \
					 		   "(nb.date - INTERVAL '1 day')::date " \
					    "END AS LastMeetingDate, " \
					 	"SUM(net_revenue) AS TotalRevenue," \
					 	"(SELECT * FROM get_biz_median(nbl.biz_id)) AS biz_median," \
					 	"(SELECT * FROM get_cm_median(nb.date)) AS cm_median," \
					 	"COUNT(CASE WHEN nbl.desig IN {} THEN 1 END) AS NumberOfFOS " \
					   "FROM nf_biz_line nbl INNER JOIN nf_biz nb ON nbl.biz_id = nb.id " \
					   "LEFT JOIN hr_employee emp ON nb.employee_id = emp.id ".format(FosDesig)

		   WhereClause = "WHERE nb.user_id = {}".format(dbm[0])
		   GroupByClause = "GROUP BY nbl.biz_id, nb.date, emp.name_related, emp.nf_emp, emp.branch_id, emp.work_email"	
		   OrderClause = "ORDER BY nb.date DESC LIMIT 7"

		   cur.execute("{} {} {} {}".format(str_sql, WhereClause, GroupByClause, OrderClause))
		   bm_dincharya_details = cur.fetchall()
		   columns = ('DincharyaDate', 'BM', 'EmpId', 'Branch', 'Email', 'TeamSize', 'CountGood', 'CountAvg', 'CountBad', 'CountAbsent', 'TotalMeeting', 'LastMeetingDate', 'TotalRevenue', 'BmMeetingMedian', 'NfMeetingMedian', 'NumberOfFOS')
		   results = map(lambda x:(dict(zip(columns, x))), bm_dincharya_details)
		  
	     else:
		 str_sql = "SELECT " \
		          "nb.date AS dincharya_date, " \
		          "(SELECT name_related FROM hr_employee WHERE id = nb.employee_id) AS BM, " \
		          "nbl.emp_id, " \
		          "nbl.desig, " \
		          "emp.name_related, " \
		          "(SELECT name FROM hr_branch branch WHERE id = emp.branch_id) AS branch, " \
		          "nbl.email, " \
		          "nbl.performance, " \
		          "CASE " \
		          " WHEN nbl.performance = 'GOOD' THEN nbl.good_remark " \
		          " WHEN nbl.performance = 'AVERAGE' THEN nbl.average_remark " \
		          " WHEN nbl.performance = 'BAD' THEN nbl.bad_remark " \
		          " ELSE 'ABSENT' " \
		          "END AS Remark, " \
		          "nbl.meeting_date AS last_meeting_date, " \
		          "nbl.new_meeting_num, " \
		          "nbl.followup_meeting_num, " \
		          "nbl.plan_of_action, " \
		          "nbl.manager_suggestion, " \
		          "nbl.num_of_order, " \
		          "nbl.net_revenue " \
			  "FROM nf_biz_line nbl INNER JOIN nf_biz nb ON nbl.biz_id = nb.id " \
		          "LEFT JOIN hr_employee emp ON nbl.employee_id = emp.id "

		 WhereClause = "WHERE nbl.email = '{}'".format(email_id)

		 OrderClause = "ORDER BY nb.date DESC LIMIT 30"

		 cur.execute("{} {} {}".format(str_sql, WhereClause, OrderClause))
		 fos_dincharya_details = cur.fetchall()
		 columns = ('DincharyaDate', 'BM', 'EmpId', 'Desig', 'EmployeeName', 'Branch', 'Email', 'Performance', 'Remark', 'LastMeetingDate', 'NewMeetingNum', 'FolloupMeetingNum', 'PlanOfAction', 'ManagerSuggestion', 'NumOfOrderTillDate', 'NetRevenueTillDate')
		 results = map(lambda x:(dict(zip(columns, x))), fos_dincharya_details)
	    
	     res = json.dumps(results, cls=PostgresJsonEncoder)
	     conn.close()
	     return Response(res, content_type='application/json; charset=utf-8')
	 except Exception as e:
	        conn.close()
                return str(e)


#getSOId
    def getSOErpId(self, soRef):
	 try:
		 conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
		 cur = conn.cursor()
	 except Exception as e:
		 return str(e)

	 try:
		 cur.execute("SELECT id FROM sale_order WHERE name = '{}'".format(soRef))
		 so_id = cur.fetchone()[0]
		 res = json.dumps(so_id, cls=PostgresJsonEncoder)
		 conn.close()
		 return Response(res, content_type='application/json; charset=utf-8')
	 except Exception as e:
		 conn.close()
		 ErrMsg = "No S.O. found in ERP"
		 res = json.dumps(ErrMsg, cls=PostgresJsonEncoder)
		 conn.close()
		 return Response(res, content_type='application/json; charset=utf-8')



#Claim MarketPlace Invoice
    def claimMarketPlaceInvoice(self,transactionId,email):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	    try:
		cur.execute("select id from res_users where login=%s and active='t'",(email,))
		sp_id = cur.fetchone()[0]
		cur.execute("select em.id, em.branch_id, em.cost_centr from hr_employee em,resource_resource re where re.id=em.resource_id and re.active='t' and re.user_id=%s",(sp_id,))
		sp_details=cur.fetchall()
		sp_emp_id = sp_details[0][0]
		sp_branch = sp_details[0][1]
	        sp_cost_center = sp_details[0][2]
	    except:
		return "CHC OR SALES PERSON EMAIL ID DOESNOT EXISTS"
	    try:
		cur.execute("select id, move_id from account_invoice where marketplace_server_id = %s AND mps_claim IS NOT TRUE",(transactionId,)) #change name with transaction id
		int_details = cur.fetchone()
		invoice_id = int_details[0]
		move_id = int_details[1]
	    except:
		return "NO INVOICE EXISTING WITH THE GIVEN MARKETPLACE SERVER ID OR SALE ALREADY CLAIMED"

		
	    cur.execute("update account_invoice set user_id = %s, c_sales_user_id = %s, c_sales_per_br = %s, mps_claim = True  where id=%s",(sp_id,sp_id,sp_branch,invoice_id,))
	    conn.commit()
	    cur.execute("update account_move_line set analytic_account_id = %s where move_id = %s AND analytic_account_id IS NOT NULL",(sp_cost_center, move_id))
	    conn.commit()
	    cur.execute("update account_invoice_line set c_sp_emp_id = %s, c_sp_branch = %s, account_analytic_id = %s where invoice_id = %s",(sp_emp_id,sp_branch, sp_cost_center, invoice_id,))
	    results = "Success"
	    cur.execute("select c_sales_order_id from account_invoice where id=%s",(invoice_id,))
	    so_id = cur.fetchone()[0]
	    if so_id:
		cur.execute("update sale_subscription_line set c_sales_person_id=%s,c_sp_branch=%s, c_cost_center = %s where c_sale_order_id=%s",(sp_id, sp_branch, sp_cost_center, so_id))
		conn.commit()
		cur.execute("update sale_order set user_id=%s,c_sp_branch=%s, project_id = %s where id=%s",(sp_id, sp_branch, sp_cost_center, so_id))
		conn.commit()
		cur.execute("update sale_order_line set salesman_id = %s where order_id=%s",(sp_id, so_id,))
		conn.commit()

	    res = json.dumps(results, cls=PostgresJsonEncoder)
	    return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


    #updateMPCServerId
    def updateMPCServerId(self, invoiceId, serverId):
	 try:
	     conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
	     cur = conn.cursor()
	 except Exception as e:
		 return str(e)

	 try:
		 cur.execute("SELECT id FROM account_invoice WHERE id = {}".format(invoiceId))
		 invoiceid = cur.fetchone()[0]
                 cur.execute("UPDATE account_invoice SET marketplace_server_id = '{}' WHERE id = {}".format(serverId, invoiceId))
	         conn.commit()
		 message = "Success"
		 res = json.dumps(message, cls=PostgresJsonEncoder)
		 conn.close()
		 return Response(res, content_type='application/json; charset=utf-8')
	 except Exception as e:
		 conn.close()
		 ErrMsg = "No Invoice found in ERP"
		 res = json.dumps(ErrMsg, cls=PostgresJsonEncoder)
		 conn.close()
		 return Response(res, content_type='application/json; charset=utf-8')


#GET PAYMENT JOURNALS
    def paymentJournals(self):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()

            query = "SELECT id,name from account_journal where type='bank'"
            cur.execute(query)

            results = []
            columns = ('id','name')
            for row in cur.fetchall():
                json_row = (dict(zip(columns, row)))
                results.append(json_row)
            res = json.dumps(results, cls=PostgresJsonEncoder)
            return Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)


#getHotProspect
    def getHotProspect(self, email_id):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)

	try:
	   cur.execute("SELECT id FROM res_users WHERE login = %s",(email_id,))
	   user_id = cur.fetchone()[0]
	except:
	   conn.close()
	   return "No email id OR email id is invalid"
	
	try:
	   cur.execute("SELECT name FROM crm_lead WHERE user_id = %s AND type = 'opportunity' AND c_quotation_created = False ORDER BY id DESC",(user_id,))
	   opp_data = cur.fetchall()
	   results = map(lambda x: x[0], opp_data)
	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            return str(e)


   #getSoByFPtags
    def getSoByFPtags(self, fptags):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)
	
	try:
	   cur.execute("SELECT " \
    			"fp.name, " \
		        "so.id, " \
		        "so.name, " \
		        "so.confirmation_date + INTERVAL '330 minutes', " \
		        "so.state " \
		        "FROM " \
		        "sale_order_line sol LEFT JOIN sale_order so ON sol.order_id = so.id " \
		        "LEFT JOIN ouc_fptag fp ON sol.c_fptags_id = fp.id " \
		        "WHERE fp.name = ANY(ARRAY{}) AND so.state IN ('sale', 'done') "\
			"ORDER BY so.confirmation_date DESC"\
  			.format(fptags))
	   data = cur.fetchall()
	   col = ('fpTag', 'soId', 'saleOrder', 'saleOrderDate', 'state')
	   results = map(lambda x: dict(zip(col,x)),  data)
	   res = json.dumps(results, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            return str(e)


   #getSoByFPtags
    def getFptagsforCreateQuotation(self, opp_id):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
	except Exception as e:
            return str(e)

	try:
	   cur.execute("SELECT partner_id FROM crm_lead WHERE id = {}".format(opp_id))
	   partner_id = cur.fetchone()[0]

	except:
	   return "Partner does not exist"
	
	try:
           res = {}
           
           cur.execute("UPDATE "
                        "ouc_fptag "
                        "SET customer_id = {}, state = 'valid' "
                        "WHERE lead_id = {}"
                        .format(partner_id, opp_id))
	   conn.commit()

           cur.execute("SELECT "
                            "name "
                            "FROM ouc_fptag "
                            "WHERE customer_id = {} AND name IS NOT NULL"
                            .format(partner_id))
           fptags = cur.fetchall()

           for val in fptags:
               res.update({str(val[0]): val[0]})
	   res = json.dumps(res, cls=PostgresJsonEncoder)
	   conn.close()
	   return Response(res, content_type='application/json; charset=utf-8')
	except Exception as e:
	    conn.close()
            return str(e)


#GET SALE ORDER NUMBER FOR COUPON CODE
    def getSaleOrder(self,coupon_code):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
            cur.execute("SELECT name from sale_order where c_discount_coupon_code=%s",(coupon_code,))
            sale_order_number = cur.fetchall()
            if len(sale_order_number) == 0:
                return "There is not sale order mapped to the given coupon code"
            else:
                results = []
                for row in sale_order_number:
                    columns = ('saleorder')
                    json_row = (columns, row)
                    results.append(json_row)
                    res = json.dumps(dict(results), cls=PostgresJsonEncoder)
                    return  Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)

#GET COUPON CODE FOR SALE ORDER NUMBER
    def getCouponCode(self,saleOrderNumber):
        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
            cur = conn.cursor()
            cur.execute("SELECT c_discount_coupon_code from sale_order where name=%s",(saleOrderNumber,))
            coupon_code = cur.fetchall()
            if len(coupon_code) == 0:
                return "There is no Coupon Code mapped to the given Sale Order"
            else:
                results = []
                for row in coupon_code:
                    columns = ('coupon_code')
                    json_row = (columns, row)
                    results.append(json_row)
                    res = json.dumps(dict(results), cls=PostgresJsonEncoder)
                    return  Response(res, content_type='application/json; charset=utf-8')
        except Exception as e:
            return str(e)
