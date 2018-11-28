using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ExpressBase.ServiceStack.Services;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Restrict(InternalOnly = true)]
    public class ReportInternalService : EbMqBaseService
    {
        public ReportInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }
        public ReportInternalResponse Post(ReportInternalRequest request)
        {
            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.JobArgs.SolnId, this.Redis);
            var objservice = base.ResolveService<EbObjectService>();
            objservice.EbConnectionFactory = ebConnectionFactory;
            var reportservice = base.ResolveService<ReportService>();
            reportservice.EbConnectionFactory = ebConnectionFactory;
            var schedulerservice = base.ResolveService<SchedulerServices>();
            schedulerservice.EbConnectionFactory = ebConnectionFactory;
            EbObjectFetchLiveVersionResponse res = ((EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest() { Id = request.JobArgs.ObjId }));
            if (res.Data.Count > 0)
            {
                EbObjectWrapper Live_ver = res.Data[0];
                GetUserEmailsResponse mailres = schedulerservice.Get(new GetUserEmailsRequest()
                {
                    UserIds = request.JobArgs.ToUserIds,
                    UserGroupIds = request.JobArgs.ToUserGroupIds
                });
                var MailIds = mailres.UserEmails.Concat(mailres.UserGroupEmails).GroupBy(d => d.Key).ToDictionary(d => d.Key, d => d.First().Value);
                foreach (var u in MailIds)
                {
                    User _readinguser = this.Redis.Get<User>(string.Format(TokenConstants.SUB_FORMAT, request.JobArgs.SolnId, u.Value, "uc"));
                    var RepRes = reportservice.Get(new ReportRenderRequest
                    {
                        Refid = Live_ver.RefId,
                        RenderingUser = new User { FullName = "MQ" },
                        ReadingUser = _readinguser,
                        Params = request.JobArgs.Params
                    });
                    RepRes.StreamWrapper.Memorystream.Position = 0;
                    MessageProducer3.Publish(new EmailServicesRequest()
                    {
                        From = "request.from",
                        To = /*ebEmailTemplate.To*/ "donaullattil93@gmail.com",
                        Cc = /*ebEmailTemplate.Cc.Split(",")*/ null,
                        Bcc = /*ebEmailTemplate.Bcc.Split(",")*/ null,
                        Message = "ebEmailTemplate.Body",
                        Subject = "ebEmailTemplate.Subject",
                        UserId = request.JobArgs.UserId,
                        UserAuthId = request.JobArgs.UserAuthId,
                        SolnId = request.JobArgs.SolnId,
                        AttachmentReport = RepRes.ReportBytea,
                        AttachmentName = RepRes.ReportName
                    });
                }            }
            return new ReportInternalResponse() { };
        }
    }
}
