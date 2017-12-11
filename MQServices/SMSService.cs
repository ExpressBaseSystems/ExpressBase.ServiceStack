using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using ExpressBase.Objects;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;

namespace ExpressBase.ServiceStack.MQServices
{
    public class SMSService : EbBaseService
    {
        public SMSService(IMessageProducer _mqp) : base(_mqp) { }

        [Authenticate]
        public void Post(SMSSentRequest request)
        {

            try
            {
                this.MessageProducer3.Publish(new SMSSentMqRequest { To = request.To, From = request.From, Body = request.Body, TenantAccountId = request.TenantAccountId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                //return true;
            }
            catch (Exception e)
            {
                //return false;
            }
        }

        [Restrict(InternalOnly = true)]
        public class SMSServiceInternal : EbBaseService
        {
            public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp) { }

            public string Post(SMSSentMqRequest req)
            {
                TenantDbFactory dbFactory = new TenantDbFactory(req.TenantAccountId, this.Redis);

                var MsgStatus = dbFactory.SMSService.SentSMS(req.To, req.From, req.Body);

                SMSStatusLogMqRequest logMqRequest = new SMSStatusLogMqRequest();
                logMqRequest.SMSSentStatus = new SMSSentStatus();

                foreach (var Info in MsgStatus)
                {
                    if (Info.Key == "To")
                        logMqRequest.SMSSentStatus.To = Info.Value;
                    if (Info.Key == "From")
                        logMqRequest.SMSSentStatus.From = Info.Value;
                    if (Info.Key == "Uri")
                        logMqRequest.SMSSentStatus.Uri = Info.Value;
                    if (Info.Key == "Body")
                        logMqRequest.SMSSentStatus.Body = Info.Value;
                    if (Info.Key == "Status")
                        logMqRequest.SMSSentStatus.Status = Info.Value;
                    if (Info.Key == "SentTime")
                        //logMqRequest.SMSSentStatus.SentTime = DateTime.Parse(Info.Value);
                    if (Info.Key == "ErrorMessage")
                        logMqRequest.SMSSentStatus.ErrorMessage = Info.Value;
                }
                logMqRequest.UserId = req.UserId;
                logMqRequest.TenantAccountId = req.TenantAccountId;

                this.MessageProducer3.Publish(logMqRequest);
                return null;
            }

            public string Post(SMSStatusLogMqRequest request)
            {
                TenantDbFactory tenantDbFactory = new TenantDbFactory(request.TenantAccountId, this.Redis);

                string sql = "INSERT INTO logs_sms(uri, send_to, send_from, message_body, status, error_message, user_id, context_id) VALUES (@uri, @to, @from, @message_body, @status, @error_message, @user_id, @context_id) RETURNING id";

                DbParameter[] parameters = 
                        {
                        tenantDbFactory.DataDB.GetNewParameter("uri", System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Uri)?string.Empty:request.SMSSentStatus.Uri),
                        tenantDbFactory.DataDB.GetNewParameter("to",System.Data.DbType.String, request.SMSSentStatus.To),
                        tenantDbFactory.DataDB.GetNewParameter("from",System.Data.DbType.String, request.SMSSentStatus.From),
                        tenantDbFactory.DataDB.GetNewParameter("message_body",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Body)?string.Empty:request.SMSSentStatus.Body),
                        tenantDbFactory.DataDB.GetNewParameter("status",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Status)?string.Empty:request.SMSSentStatus.Status),
                        //tenantDbFactory.DataDB.GetNewParameter("sent_time",System.Data.DbType.DateTime, request.SMSSentStatus.SentTime),
                        tenantDbFactory.DataDB.GetNewParameter("error_message",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.ErrorMessage)?string.Empty:request.SMSSentStatus.ErrorMessage),
                        tenantDbFactory.DataDB.GetNewParameter("user_id",System.Data.DbType.Int32, request.UserId),
                        tenantDbFactory.DataDB.GetNewParameter("context_id",System.Data.DbType.Int32, string.IsNullOrEmpty(request.ContextId.ToString())?request.UserId:request.ContextId)
                        };
                var iCount = tenantDbFactory.DataDB.DoQuery(sql, parameters);

                return null;
            }
        }
    }
}
