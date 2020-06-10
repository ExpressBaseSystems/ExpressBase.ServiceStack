using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using ExpressBase.Objects;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Objects.SmsRelated;
using ExpressBase.Common;
using System.Text.RegularExpressions;
using ExpressBase.Objects.Services;

namespace ExpressBase.ServiceStack.MQServices
{
    [Authenticate]
    public class SmsCreateService : EbBaseService
    {
        public SmsCreateService(IMessageProducer _mqp) : base(_mqp) { }

        public void Post(SMSInitialRequest request)
        {
            this.MessageProducer3.Publish(new SMSPrepareRequest
            {
                ObjId = request.ObjId,
                Params = request.Params,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                MediaUrl = request.MediaUrl,
                RefId = request.RefId
            });
        }
    }

    [Restrict(InternalOnly = true)]
    public class SMSService : EbMqBaseService
    {
        public SMSService(IMessageProducer _mqp) : base(_mqp) { }

        public void Post(SMSPrepareRequest request)
        {
            string smsTo = string.Empty;
            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            EbObjectService objservice = base.ResolveService<EbObjectService>();
            objservice.EbConnectionFactory = ebConnectionFactory;
            // EbObjectFetchLiveVersionResponse res = (EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest() { Id = request.ObjId });
            EbSmsTemplate SmsTemplate = new EbSmsTemplate();
            if (request.ObjId > 0)
            {
                EbObjectFetchLiveVersionResponse template_res = (EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest() { Id = request.ObjId });
                if (template_res != null && template_res.Data.Count > 0)
                {
                    SmsTemplate = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }
            else if (request.RefId != string.Empty)
            {
                EbObjectParticularVersionResponse template_res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                if (template_res != null && template_res.Data.Count > 0)
                {
                    SmsTemplate = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }

            if (SmsTemplate != null)
            {
                if (SmsTemplate.DataSourceRefId != string.Empty && SmsTemplate.To != string.Empty && SmsTemplate.To != null)
                {
                    EbObjectParticularVersionResponse myDsres = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = SmsTemplate.DataSourceRefId });
                    if (myDsres.Data.Count > 0)
                    {
                        EbDataReader reader = new EbDataReader();
                        reader = EbSerializers.Json_Deserialize(myDsres.Data[0].Json);
                        IEnumerable<DbParameter> parameters = DataHelper.GetParams(ebConnectionFactory, false, request.Params, 0, 0);
                        EbDataSet ds = ebConnectionFactory.ObjectsDB.DoQueries(reader.Sql, parameters.ToArray());
                        string pattern = @"\{{(.*?)\}}";
                        IEnumerable<string> matches = Regex.Matches(SmsTemplate.Body, pattern).OfType<Match>()
                         .Select(m => m.Groups[0].Value)
                         .Distinct();
                        foreach (string _col in matches)
                        {
                            string str = _col.Replace("{{", "").Replace("}}", "");

                            foreach (EbDataTable dt in ds.Tables)
                            {
                                string colname = dt.Rows[0][str.Split('.')[1]].ToString();
                                SmsTemplate.Body = SmsTemplate.Body.Replace(_col, colname);
                            }
                        }
                        foreach (var dt in ds.Tables)
                        {
                            smsTo = dt.Rows[0][SmsTemplate.To.Split('.')[1]].ToString();
                        }
                    }
                    try
                    {
                        this.MessageProducer3.Publish(new SMSSentRequest
                        {
                            To = smsTo,
                            Body = SmsTemplate.Body,
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            WhichConsole = request.WhichConsole,
                            UserAuthId = request.UserAuthId
                        });
                        //return true;
                    }
                    catch (Exception e)
                    {
                        Log.Info("Exception in SMSSentRequest publish to " + smsTo + e.Message + e.StackTrace);
                        //return false;
                    }
                }
            }
        }

        //[Route("/callback/{apikey}")]
        //public class String : IReturn<IReturnVoid>
        //{
        //    public String apikey { get; set; }
        //}


        //public void Any(SMSService.String apikey)
        //{

        // }

        //[Restrict(InternalOnly = true)]
        //public class SMSServiceInternal : EbBaseService
        //{
        //    public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp) { }

        //    public string Post(SMSSentMqRequest req)
        //    {
        //        EbConnectionFactory dbFactory = new EbConnectionFactory(req.TenantAccountId, this.Redis);

        //        var MsgStatus = dbFactory.SMSConnection.SendSMS(req.To, req.From, req.Body);

        //        SMSStatusLogMqRequest logMqRequest = new SMSStatusLogMqRequest();
        //        logMqRequest.SMSSentStatus = new SMSSentStatus();

        //        foreach (var Info in MsgStatus)
        //        {
        //            if (Info.Key == "To")
        //                logMqRequest.SMSSentStatus.To = Info.Value;
        //            if (Info.Key == "From")
        //                logMqRequest.SMSSentStatus.From = Info.Value;
        //            if (Info.Key == "Uri")
        //                logMqRequest.SMSSentStatus.Uri = Info.Value;
        //            if (Info.Key == "Body")
        //                logMqRequest.SMSSentStatus.Body = Info.Value;
        //            if (Info.Key == "Status")
        //                logMqRequest.SMSSentStatus.Status = Info.Value;
        //            if (Info.Key == "SentTime")
        //                //logMqRequest.SMSSentStatus.SentTime = DateTime.Parse(Info.Value);
        //                if (Info.Key == "ErrorMessage")
        //                    logMqRequest.SMSSentStatus.ErrorMessage = Info.Value;
        //        }
        //        logMqRequest.UserId = req.UserId;
        //        logMqRequest.TenantAccountId = req.TenantAccountId;

        //        this.MessageProducer3.Publish(logMqRequest);
        //        return null;
        //    }

        //    public string Post(SMSStatusLogMqRequest request)
        //    {
        //        EbConnectionFactory connectionFactory = new EbConnectionFactory(request.TenantAccountId, this.Redis);

        //        string sql = "INSERT INTO logs_sms(uri, send_to, send_from, message_body, status, error_message, user_id, context_id) VALUES (@uri, @to, @from, @message_body, @status, @error_message, @user_id, @context_id) RETURNING id";

        //        DbParameter[] parameters =
        //                {
        //                connectionFactory.DataDB.GetNewParameter("uri", EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Uri)?string.Empty:request.SMSSentStatus.Uri),
        //                connectionFactory.DataDB.GetNewParameter("to",EbDbTypes.String, request.SMSSentStatus.To),
        //                connectionFactory.DataDB.GetNewParameter("from",EbDbTypes.String, request.SMSSentStatus.From),
        //                connectionFactory.DataDB.GetNewParameter("message_body",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Body)?string.Empty:request.SMSSentStatus.Body),
        //                connectionFactory.DataDB.GetNewParameter("status",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Status)?string.Empty:request.SMSSentStatus.Status),
        //                //connectionFactory.DataDB.GetNewParameter("sent_time",System.Data.DbType.DateTime, request.SMSSentStatus.SentTime),
        //                connectionFactory.DataDB.GetNewParameter("error_message",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.ErrorMessage)?string.Empty:request.SMSSentStatus.ErrorMessage),
        //                connectionFactory.DataDB.GetNewParameter("user_id",EbDbTypes.Int32, request.UserId),
        //                connectionFactory.DataDB.GetNewParameter("context_id",EbDbTypes.Int32, string.IsNullOrEmpty(request.ContextId.ToString())?request.UserId:request.ContextId)
        //                };
        //        var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);

        //        return null;
        //    }
        //}
    }
}
