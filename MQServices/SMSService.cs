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

        [Route("/callback/{apikey}")]
        public class String : IReturn<IReturnVoid>
        {
            public String apikey { get; set; }
        }

        
        public void Any(SMSService.String apikey)
        {

        }

        [Restrict(InternalOnly = true)]
        public class SMSServiceInternal : EbBaseService
        {
            public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp) { }

            public string Post(SMSSentMqRequest req)
            {
                EbConnectionFactory dbFactory = new EbConnectionFactory(req.TenantAccountId, this.Redis);

                var MsgStatus = dbFactory.SMSConnection.SendSMS(req.To, req.From, req.Body);

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
                EbConnectionFactory connectionFactory = new EbConnectionFactory(request.TenantAccountId, this.Redis);

                string sql = "INSERT INTO logs_sms(uri, send_to, send_from, message_body, status, error_message, user_id, context_id) VALUES (@uri, @to, @from, @message_body, @status, @error_message, @user_id, @context_id) RETURNING id";

                DbParameter[] parameters = 
                        {
                        connectionFactory.DataDB.GetNewParameter("uri", System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Uri)?string.Empty:request.SMSSentStatus.Uri),
                        connectionFactory.DataDB.GetNewParameter("to",System.Data.DbType.String, request.SMSSentStatus.To),
                        connectionFactory.DataDB.GetNewParameter("from",System.Data.DbType.String, request.SMSSentStatus.From),
                        connectionFactory.DataDB.GetNewParameter("message_body",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Body)?string.Empty:request.SMSSentStatus.Body),
                        connectionFactory.DataDB.GetNewParameter("status",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.Status)?string.Empty:request.SMSSentStatus.Status),
                        //connectionFactory.DataDB.GetNewParameter("sent_time",System.Data.DbType.DateTime, request.SMSSentStatus.SentTime),
                        connectionFactory.DataDB.GetNewParameter("error_message",System.Data.DbType.String, string.IsNullOrEmpty(request.SMSSentStatus.ErrorMessage)?string.Empty:request.SMSSentStatus.ErrorMessage),
                        connectionFactory.DataDB.GetNewParameter("user_id",System.Data.DbType.Int32, request.UserId),
                        connectionFactory.DataDB.GetNewParameter("context_id",System.Data.DbType.Int32, string.IsNullOrEmpty(request.ContextId.ToString())?request.UserId:request.ContextId)
                        };
                var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);

                return null;
            }
        }
    }
}
