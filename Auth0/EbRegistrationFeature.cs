
using ExpressBase.Common;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.FluentValidation;
using ServiceStack.Validation;
using ServiceStack.Web;
using System;
using System.Data.Common;
using System.Globalization;

namespace ExpressBase.ServiceStack.Auth0
{
    /// <summary>
    /// Enable the Registration feature and configure the RegistrationService.
    /// </summary>
    public class EbRegistrationFeature : IPlugin
    {
        public string AtRestPath { get; set; }

        public EbRegistrationFeature()
        {
            this.AtRestPath = "/register";
        }

        public void Register(IAppHost appHost)
        {
            appHost.RegisterService<EbRegistrationFeature>(AtRestPath);
            appHost.RegisterAs<RegistrationValidator, IValidator<Register>>();
        }
    }

    public class EbRegisterServices : RegisterService
    {
        public new object Post(Register request)
        { 
            var response = base.Post(request) as RegisterResponse;
            var _InfraDb = base.TryResolve<TenantDbFactory>().DataDB as IDatabase;
            DbParameter[] parameters = {
                _InfraDb.GetNewParameter("cname", System.Data.DbType.String, request.Email),
                _InfraDb.GetNewParameter("password", System.Data.DbType.String, request.Password)
            };
            EbDataTable dt = _InfraDb.DoQuery("INSERT INTO eb_tenants (cname,password,u_token) VALUES ( @cname,@password,md5( @cname || now())) RETURNING id,u_token;", parameters);

            //var authRepo = HostContext.AppHost.GetAuthRepository(base.Request);
            //authRepo.DeleteUserAuth(response.UserId);                               // we dont want servicestack authuser.id in redis
            //var existingUser1 = authRepo.GetUserAuth(base.GetSession(), null);
            //var existingUser2 = authRepo.GetUserAuth(base.GetSession(), null);
            //existingUser2.Id = Convert.ToInt32(dt.Rows[0][0]);
            //authRepo.UpdateUserAuth(existingUser1, existingUser2, request.Password);

            response.UserName = dt.Rows[0][1].ToString();
            response.UserId = dt.Rows[0][0].ToString();
            return response;
        }
    }
}