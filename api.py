from flask import Flask
from flask import request
from helpers.postgresconnector import *

#initialize Flask
app = Flask(__name__)

#clientId
#1.android clientId
validClientIds = ['ERPWE0892F2F45388F439BDE9F6F3FB5C31F0FAA628D40CD2920A79D8841597B']

#@app.route('/echo', methods = ['GET', 'POST', 'PATCH', 'PUT', 'DELETE'])
@app.route('/', methods = ['GET','POST','PUT'])
def ping():
    return 'Welcome'


@app.route('/checkdb', methods = ['GET'])
def checkdb():
    try:
	return PostgresConnector().ConnectToDatabase()
    except Exception as e:
	return str(e)

#APIs using

@app.route('/v1/CheckUser', methods = ['POST'])
def UserCheck():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    loginKey = data['loginKey']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    if (loginKey and loginKey.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400
    try:
	return PostgresConnector().GetUserId(loginKey)
    except Exception as e:
	print str(e)

@app.route('/v1/GetLeadDetails', methods = ['POST'])
def GetLeadDetails():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    leadName = data['leadName']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    if (leadName and leadName.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    return PostgresConnector().GetLeadDetails(leadName)

@app.route('/v1/GetAllInvoices', methods = ['POST'])
def GetAllInvoices():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().GetAllInvoices()

@app.route('/v1/FHTemp', methods = ['POST'])
def FHTemp():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().FHTemp()

@app.route('/v1/FHManagers', methods = ['POST'])
def FHManagers():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().FHManagers()

@app.route('/v1/SPEmail', methods = ['POST'])
def SPEmail():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    refNumber = data['refNumber']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (refNumber and refNumber.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    return PostgresConnector().SPEmail(refNumber)

@app.route('/v1/getLeadList', methods = ['POST'])
def getLeadList():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    spEmail = data['spEmail']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (spEmail and spEmail.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    return PostgresConnector().getLeadList(spEmail)


@app.route('/v1/CreateMeeting', methods = ['POST'])
def CreateMeeting():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    userlogin = data['userlogin']
    leadNumber = data['leadNumber']
    description = data['description']
    meetingstatus = data['meetingstatus']#interested,not interested,followup
    concernedperson = data['concernedperson']#yes/no
    fptag = data['fptag']
    meetingType = data['meetingType']#demo/normal

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    if (userlogin and userlogin.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    try:
	return PostgresConnector().CreateMeeting(userlogin,leadNumber,description,meetingstatus,concernedperson,fptag,meetingType)
    except Exception as e:
	print str(e)


@app.route('/v1/getHierarchy', methods = ['POST'])
def getHierarchy():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    empEmail = data['empEmail']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (empEmail and empEmail.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    return PostgresConnector().getHierarchy(empEmail)

@app.route('/v1/getEmployeeCount', methods = ['POST'])
def getEmployeeCount():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().getEmployeeCount()

@app.route('/v1/getFPHandleList', methods = ['POST'])
def getFPHandleList():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    FPTag = data['FPTag']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (FPTag and FPTag.isspace()):
        return "PARAMETERS CANNOT BE EMPTY", 400

    return PostgresConnector().getFPHandleList()

@app.route('/v1/GetPackageExtensions', methods = ['POST'])
def GetPackageExtensions():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().GetPackageExtensions()

@app.route('/v1/GetActiveEmployees', methods = ['POST'])
def GetActiveEmployees():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().GetActiveEmployees()

@app.route('/v1/GetSpecificEmployees', methods = ['POST'])
def GetSpecificEmployees():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    email = data['email']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    if (email and email.isspace()):
        return "INVALID CLIENT ID", 400

    return PostgresConnector().GetSpecificEmployees(email)



@app.route('/v1/GetLatestInvoice', methods = ['POST'])
def GetLatestInvoice():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    fptag = data['fptag']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (fptag and fptag.isspace()):
        return "INVALID FPTAG", 400

    return PostgresConnector().GetLatestInvoice(fptag)

@app.route('/v1/updateCHC', methods = ['POST'])
def updateCHC():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    fptag = data['fptag']
    email = data['email']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (fptag and fptag.isspace()):
        return "INVALID INVOICE NUMBER", 400
    if (email and email.isspace()):
        return "INVALID EMAIL ID", 400
    return PostgresConnector().updateCHC(fptag,email)

@app.route('/v1/getPIList', methods = ['POST'])
def getPIList():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    email = data['email']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (email and email.isspace()):
        return "INVALID EMAIL ID", 400

    return PostgresConnector().getPIList(email)

@app.route('/v1/getPICustomerList', methods = ['POST'])
def getPICustomerList():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    email = data['email']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (email and email.isspace()):
        return "INVALID EMAIL ID", 400

    return PostgresConnector().getPICustomerList(email)

@app.route('/v1/getPIListForCustomer', methods = ['POST'])
def getPIListForCustomer():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    email = data['email']
    customerName = data['customerName']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (email and email.isspace()):
        return "INVALID EMAIL ID", 400
    if (customerName and customerName.isspace()):
        return "INVALID CUSTOMER NAME", 400

    return PostgresConnector().getPIListForCustomer(email,customerName)

@app.route('/v1/getPIDetails', methods = ['POST'])
def getPIDetails():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    refNumber = data['refNumber']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (refNumber and refNumber.isspace()):
        return "INVALID REFERENCE NUMBER", 400

    return PostgresConnector().getPIDetails(refNumber)


@app.route('/v1/getPreviousSale', methods = ['POST'])
def getPreviousSale():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    fptag = data['fptag']
    packageid = data['packageid']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (fptag and fptag.isspace()):
        return "INVALID FPTAG", 400
    if (packageid and packageid.isspace()):
        return "INVALID PACKAGEID", 400

    return PostgresConnector().getPreviousSale(fptag,packageid)


@app.route('/v1/updateDiscount', methods = ['POST'])
def updateDiscount():
    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    lineid = data['lineid']
    discount = data['discount']
    approvalstatus = data.get('approvalstatus', "")
    coupon_code = data.get('coupon_code', "")
    coupon_validity = data.get('coupon_validity', "")
    isSingleStore = data.get('isSingleStore', False)
   
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400

    pcObj = PostgresConnector()	
    if isSingleStore:
	result = pcObj.updateSingleStoreDiscount(lineid,discount,approvalstatus,coupon_code,coupon_validity)
    else:
	result = pcObj.updateDiscount(lineid,discount,approvalstatus,coupon_code,coupon_validity)

    return result

@app.route('/v1/getnoCHCDetails', methods = ['POST'])
def getnoCHCDetails():
    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    return PostgresConnector().getnoCHCDetails()

@app.route('/v1/getLeadNumberforCHC', methods = ['POST'])
def getLeadNumberforCHC():
    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    customeremail = data['customeremail']
    spemail = data['spemail']
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (customeremail and customeremail.isspace()):
        return "INVALID CUSTOMER EMAIL ID", 400
    if (spemail and spemail.isspace()):
        return "INVALID CHC EMAIL ID", 400
    return PostgresConnector().getLeadNumberforCHC(customeremail,spemail)


@app.route('/v1/listOfLatestInvoice', methods = ['POST'])
def listOfLatestInvoice():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    fptags = data['fptags']

    return PostgresConnector().listOfLatestInvoice(fptags)

@app.route('/v1/sendROIEnquiryEmail', methods = ['GET'])
def send_roi_enquiry_email():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    fptag = json_data.get('fptag',False) and json_data.get('fptag',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    if not fptag:
        return "INVALID FPTAG or NO FPTAG", 400	
    return PostgresConnector().send_roi_enquiry_email(fptag)

@app.route('/v1/ThdAchievement', methods = ['GET'])
def ThdAchievement():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    emailId = json_data.get('emailId',False) and json_data.get('emailId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().ThdAchievement(emailId)

@app.route('/v1/MeetingData', methods = ['GET'])
def MeetingData():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    emailId = json_data.get('emailId',False) and json_data.get('emailId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().MeetingData(emailId)

@app.route('/v1/FosRank', methods = ['GET'])
def FosRank():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    emailId = json_data.get('emailId',False) and json_data.get('emailId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().FosRank(emailId)

@app.route('/v1/ExotelIncomingCall', methods = ['GET'])
def ExotelIncomingCall():
    json_data = dict(request.args)
    print"====json_data=====",json_data
    CallSid = json_data.get('CallSid',False) and json_data.get('CallSid',False)[0] or ''
    CallFrom = json_data.get('CallFrom',False) and json_data.get('CallFrom',False)[0] or ''
    CallTo = json_data.get('CallTo',False) and json_data.get('CallTo',False)[0] or ''
    CallStatus = json_data.get('CallStatus',False) and json_data.get('CallStatus',False)[0] or ''
    Direction = json_data.get('Direction',False) and json_data.get('Direction',False)[0] or ''
    return PostgresConnector().ExotelIncomingCall(CallFrom, CallTo)

@app.route('/v1/DincharyaPerformance', methods = ['GET'])
def DincharyaPerformance():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    emailId = json_data.get('emailId',False) and json_data.get('emailId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().DincharyaPerformance(emailId)

@app.route('/v1/getSOErpId', methods = ['GET'])
def getSOErpId():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    soRef = json_data.get('soRef',False) and json_data.get('soRef',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().getSOErpId(soRef)


@app.route('/v1/claimMarketPlaceInvoice', methods = ['POST'])
def claimMarketPlaceInvoice():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    transactionId = data['transactionId']
    email = data['email']
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    if (transactionId and transactionId.isspace()):
        return "INVALID TRANSACTION ID or NO CLIENT ID", 400
    if (email and email.isspace()):
        return "INVALID EMAIL ID or NO CLIENT ID", 400

    return PostgresConnector().claimMarketPlaceInvoice(transactionId,email)

@app.route('/v1/updateMarketPlaceServerId', methods = ['POST'])
def updateMPCServerId():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    invoiceId = data['invoiceId']
    serverId = data['serverId']
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400

    return PostgresConnector().updateMPCServerId(invoiceId, serverId)

@app.route('/v1/paymentJournals', methods = ['POST'])
def paymentJournals():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400

    return PostgresConnector().paymentJournals()

@app.route('/v1/getHotProspect', methods = ['GET'])
def getHotProspect():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    emailId = json_data.get('emailId',False) and json_data.get('emailId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().getHotProspect(emailId)

@app.route('/v1/getSoByFPtags', methods = ['POST'])
def getSoByFPtags():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    
    fptags = map(lambda x: x.encode('UTF-8'), request.get_json())

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    if not fptags:
       return "Please provide FPtags"
    return PostgresConnector().getSoByFPtags(fptags)

@app.route('/v1/getFptagsforCreateQuotation', methods = ['GET'])
def getFptagsforCreateQuotation():
    json_data = dict(request.args)
    clientId = json_data.get('clientId',False) and json_data.get('clientId',False)[0] or ''
    oppId = json_data.get('oppId',False) and json_data.get('oppId',False)[0] or ''
    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID or NO CLIENT ID", 400
    return PostgresConnector().getFptagsforCreateQuotation(oppId)

@app.route('/v1/getSaleOrder', methods = ['POST'])
def getSaleOrder():

    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400

    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    coupon_code = data['coupon_code']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (coupon_code and coupon_code.isspace()):
        return "INVALID EMAIL ID", 400

    return PostgresConnector().getSaleOrder(coupon_code)

@app.route('/v1/getCouponCode', methods = ['POST'])
def getCouponCode():
    #validate the Content-Type
    if request.headers['Content-Type'] != 'application/json':
        return "UNSUPPORTED CONTENT-TYPE", 400
    #Retrieve Request Parameters
    data = request.get_json()
    clientId = data['clientId']
    saleOrderNumber = data['saleOrderNumber']

    if ((clientId and clientId.isspace()) or (clientId not in validClientIds)):
        return "INVALID CLIENT ID", 400
    if (saleOrderNumber and saleOrderNumber.isspace()):
        return "INVALID EMAIL ID", 400
    return PostgresConnector().getCouponCode(saleOrderNumber)


#run Flask
if __name__ == '__main__':
    app.run(host='0.0.0.0',port=80,debug=False,threaded=True)
