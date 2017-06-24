using ExpressBase.Data;
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
            var _InfraDb = base.TryResolve<DatabaseFactory>().InfraDB as IDatabase;
            DbParameter[] parameters = {
                _InfraDb.GetNewParameter("cname", System.Data.DbType.String, request.Email),
                _InfraDb.GetNewParameter("password", System.Data.DbType.String, request.Password)
            };
            EbDataTable dt = _InfraDb.DoQuery("INSERT INTO eb_tenants (cname,password,u_token) VALUES ( @cname,@password,md5( @cname || now())) RETURNING id,u_token;", parameters);

            response.UserId = dt.Rows[0][0].ToString();
            response.UserName = dt.Rows[0][1].ToString();
            return response;
        }
    }
}