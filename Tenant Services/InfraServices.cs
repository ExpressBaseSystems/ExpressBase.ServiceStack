using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Loader;
using ServiceStack.Messaging;
using System.Text;
using System.Globalization;
using ExpressBase.ServiceStack.MQServices;
using Newtonsoft.Json;

namespace ExpressBase.ServiceStack.Services
{
    [ClientCanSwapTemplates]
    [EnableCors]
    [Authenticate]
    public class InfraServices : EbBaseService
    {
        public InfraServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }

        public JoinbetaResponse Post(JoinbetaReq r)
        {
            JoinbetaResponse resp = new JoinbetaResponse();
            try
            {
                string sql = string.Format("INSERT INTO eb_beta_enq(email,time) values('{0}','now()') RETURNING id", r.Email);
                var f = this.InfraConnectionFactory.DataDB.DoQuery(sql);
                if (f.Rows.Count > 0)
                    resp.Status = true;
                else
                    resp.Status = false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                resp.Status = false;
            }
            return resp;
        }

        public GetVersioning Post(SetVersioning request)
        {
            GetVersioning resp = new GetVersioning();
            try
            {
                string sql = string.Format("UPDATE eb_solutions SET versioning = true WHERE solution_id = '{0}';", request.solution_id);
                int r = this.InfraConnectionFactory.DataDB.DoNonQuery(sql);
                if (r > 0)
                {
                    resp.Versioning = request.Versioning;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                resp.status.Message = e.Message;
            }
            return resp;
        }

        public CreateAccountResponse Post(CreateAccountRequest request)
        {
            CreateAccountResponse resp = new CreateAccountResponse();
            try
            {
                Console.WriteLine("Reached... inserting tenant details into table  ");
                string sql = @"INSERT INTO eb_tenants(
                                                    email,
                                                    fullname,
                                                    country,
                                                    pwd,
                                                    activation_code,
                                                    eb_created_at,
													eb_del,
													is_verified,
													is_email_sent

                                                )VALUES(
                                                    :email,
                                                    :fullname,
                                                    :country,
                                                    :pwd,
                                                    :activationcode,
                                                     NOW(),
													:fals,
													:fals,
													:fals	
                                                )RETURNING id";

                //string sql = "SELECT * FROM eb_tenantprofile_setup(:fullname, :country, :pwd, :email,:activationcode,:accounttype);";

                DbParameter[] parameters = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("fullname", EbDbTypes.String, request.Name),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("country", EbDbTypes.String, request.Country),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String, (request.Password.ToString() + request.Email.ToString()).ToMD5Hash()),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, request.Email),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("activationcode", EbDbTypes.String, request.ActivationCode),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, 'F')
                    };

                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                resp.Id = Convert.ToInt32(dt.Rows[0][0]);
                if (resp.Id > 0)
                {
                    resp.AccountCreated = true;
                    resp.Notified = this.SendTenantMail(resp.Id, request.ActivationCode, request.PageUrl, request.Name, request.Email);

                    CreateSolutionResponse response = this.Post(new CreateSolutionRequest
                    {
                        SolutionName = "My First Solution",
                        Description = "This is my first solution",
                        DeployDB = true,
                        UserId = resp.Id
                    });

                    if (response.Id > 0)
                        resp.DbCreated = true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }
            return resp;
        }

        public CreateSolutionFurtherResponse Post(CreateSolutionFurtherRequest request)
        {
            CreateSolutionFurtherResponse resp = new CreateSolutionFurtherResponse();
            int _solcount = 0;
            try
            {
                string sql = @"SELECT COUNT(*) FROM eb_solutions WHERE tenant_id = :tid";
                DbParameter[] parameters =
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("tid",EbDbTypes.Int32,request.UserId)
                };
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                _solcount = Convert.ToInt32(dt.Rows[0][0]) + 1;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error at count * of solutions :" + e.Message);
            }

            try
            {
                CreateSolutionResponse response = this.Post(new CreateSolutionRequest
                {
                    SolutionName = "My Solution " + _solcount,
                    Description = "My solution " + _solcount,
                    DeployDB = true,
                    UserId = request.UserId,
                    IsFurther = true
                });
                if (response.Id > 0)
                {
                    resp.SolId = response.Id;
                    resp.Status = true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception at new solution creation furtherRequest :" + e.Message);
                resp.Status = false;
            }
            return resp;
        }

        private string MailHtml
        {
            get
            {
                return @"<html>
<head>
    <title></title>
</head>
<body>
    <div style='border: 1px solid #508bf9;padding:20px 40px 20px 40px; '>
        <figure style='text-align: center;margin:0px;'>
            <img src='https://expressbase.com/images/logos/EB_Logo.png' /><br />
        </figure>
        <br />
        <h3 style='color:#508bf9;margin:0px'>Build Business Apps 10x faster!</h3> <br />
        <div style='line-height: 1.4;'>
            Dear {UserName},<br />
            <br />
            Welcome to EXPRESSbase, an Open-Source, Low-Code Rapid application development & delivery platform on the cloud for businesses & developers to build & run business apps 10 times faster.<br />
			 <br />
            We're excited to help you get started with your new EXPRESSbase account. Please go thru our <a href='{wikiurl}'>Wiki</a> for tutorials. 
            If you wish to connect the database used by your existing applications, you could do it in very simple  <a href='{stepsurl}'>steps</a> – and it is secure too!<br /><br />
            Just click the button below to verify your email address.<br />
        </div>
        <br />
        <table>
            <tr>
                <td class='btn-read-online' style='text-align: center; background-color: #508bf9; padding: 10px 15px; border-radius: 5px;'>
                    <a href='{Url}' style='color: #fff; font-size: 16px; letter-spacing: 1px; text-decoration: none;  font-family:Helvetica,sans-serif,Montserrat, Arial ;'>Verify Account</a>
                </td>
            </tr>
        </table>
        <br />
        If the previous button does not work, try to copy and paste the following URL in your browser’s address bar:<br />
        <a href='{Url}'>{Url}</a><br />
         <br />

        Need help? Please drop in a mail to <a href='{supporturl}'>support@expressbase.com</a>. We're right here for you.<br /><br />
        Sincerely,<br />
        Team EXPRESSbase<br />
    </div>
</body>
</html>";
            }
            set { }
        }

        private bool SendTenantMail(int tid, string activationcode, string pageurl, string name, string email)
        {

            bool status = false;
            string aq = "$" + tid + "$" + activationcode + "$";
            byte[] plaintxt = System.Text.Encoding.UTF8.GetBytes(aq);
            string ai = System.Convert.ToBase64String(plaintxt);
            string elinks2 = string.Format("https://{0}/em?emv={1}", pageurl, ai);
            string mailbody = this.MailHtml;
            mailbody = mailbody.Replace("{UserName}", name).Replace("{Url}", elinks2);
            string wikiurl = "https://myaccount.expressbase.com/wiki";
            string stepsurl = "https://myaccount.expressbase.com/Wiki/Integrations/Connecting-your-existing-Database";
            string supporturl = "mailto:support@expressbase.com";


            try
            {
                mailbody = mailbody.Replace("{UserName}", name).Replace("{Url}", elinks2).Replace("{wikiurl}", wikiurl).Replace("{supporturl}", supporturl).Replace("{stepsurl}", stepsurl);

                MessageProducer3.Publish(new EmailServicesRequest
                {
                    To = email,
                    Subject = "Welcome to EXPRESSbase",
                    Message = mailbody,
                    SolnId = CoreConstants.EXPRESSBASE,

                });
                string quer = string.Format("UPDATE eb_tenants SET is_email_sent = 'T'  WHERE id = '{0}'", tid);
                int dtb = this.InfraConnectionFactory.DataDB.DoNonQuery(quer);
                if (dtb > 0)
                    status = true;
            }
            catch (Exception e)
            {
                status = true;
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }
            return status;
        }

        //******** CREATE DEFAULT SOLUTION AND DEPLOY DATABLE FOR THE USER
        public CreateSolutionResponse Post(CreateSolutionRequest request)
        {
            EbDbCreateServices _dbService = base.ResolveService<EbDbCreateServices>();
            ConnectionManager _conService = base.ResolveService<ConnectionManager>();
            TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
            CreateSolutionResponse resp = new CreateSolutionResponse();
            try
            {
                string Sol_id_autogen = string.Empty;
                if (string.IsNullOrEmpty(request.SolnUrl))
                {
                    string sid = "SELECT * from eb_sid_gen();";
                    EbDataTable dt1 = this.InfraConnectionFactory.DataDB.DoQuery(sid);
                    Sol_id_autogen = Convert.ToString(dt1.Rows[0][0]);
                }
                else
                {
                    Sol_id_autogen = request.SolnUrl;
                }

                string sql = @"INSERT INTO eb_solutions
                                            (
                                                solution_name,
                                                tenant_id,
                                                date_created,
                                                description,
                                                solution_id,
                                                esolution_id,
                                                isolution_id,
                                                pricing_tier
                                            )
                                            VALUES(
                                                :sname,
                                                :tenant_id,
                                                now(),
                                                :descript,
                                                :solnid,
                                                :solnid,
                                                :solnid,
                                                0
                                            ) RETURNING id;	
                                INSERT INTO eb_role2tenant
                                            (
                                                tenant_id,
                                                solution_id,
                                                sys_role_id,
                                                eb_createdat,
                                                eb_createdby
                                            )
                                            VALUES
                                            (
                                                :tenant_id,
                                                :solnid,
                                                0,
                                                NOW(),
                                                :tenant_id
                                            )RETURNING id;";

                DbParameter[] parameters = new DbParameter[]
                {
                    InfraConnectionFactory.DataDB.GetNewParameter("sname", EbDbTypes.String, request.SolutionName),
                    InfraConnectionFactory.DataDB.GetNewParameter("tenant_id", EbDbTypes.Int32, request.UserId),
                    InfraConnectionFactory.DataDB.GetNewParameter("descript", EbDbTypes.String, request.Description),
                    InfraConnectionFactory.DataDB.GetNewParameter("solnid", EbDbTypes.String,Sol_id_autogen)
                };

                EbDataSet _ds = this.InfraConnectionFactory.DataDB.DoQueries(sql, parameters);
                resp.Id = Convert.ToInt32(_ds.Tables[0].Rows[0][0]);

                if (resp.Id > 0)
                {
                    if (request.DeployDB)
                    {
                        EbDbCreateResponse response = (EbDbCreateResponse)_dbService.Post(new EbDbCreateRequest
                        {
                            DBName = Sol_id_autogen,
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            IsChange = false,
                            IsFurther = request.IsFurther

                        });

                        if (response.DeploymentCompled)
                        {
                            _conService.Post(new InitialSolutionConnectionsRequest
                            {
                                NewSolnId = Sol_id_autogen,
                                SolnId = request.SolnId,
                                UserId = request.UserId,
                                DbUsers = response.DbUsers
                            });

                            _tenantUserService.Post(new UpdateSolutionRequest
                            {
                                SolnId = Sol_id_autogen,
                                UserId = request.UserId
                            });
                            if (!request.IsFurther)
                            {
                                ImportrExportService service = base.ResolveService<ImportrExportService>();
                                int demoAppId;
                                if (Environment.GetEnvironmentVariable(EnvironmentConstants.ASPNETCORE_ENVIRONMENT) == "Production")
                                    demoAppId = 9;
                                else
                                    demoAppId = 129;
                                ImportApplicationResponse _response = service.Get(new ImportApplicationMqRequest
                                {
                                    Id = demoAppId,
                                    SolnId = Sol_id_autogen,
                                    UserId = request.UserId,
                                    UserAuthId = "",
                                    WhichConsole = "",
                                    IsDemoApp = true
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }
            return resp;
        }

        public GetSolutionResponse Get(GetSolutionRequest request)
        {
            List<EbSolutionsWrapper> temp = new List<EbSolutionsWrapper>();
            string sql = string.Format("SELECT * FROM eb_solutions WHERE tenant_id={0} AND eb_del=false;", request.UserId);
            GetSolutionResponse resp = new GetSolutionResponse();
            try
            {
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
                foreach (EbDataRow dr in dt.Rows)
                {
                    EbSolutionsWrapper _ebSolutions = (new EbSolutionsWrapper
                    {
                        SolutionName = dr[6].ToString(),
                        Description = dr[2].ToString(),
                        DateCreated = Convert.ToDateTime(dr[1]).ToString("g", DateTimeFormatInfo.InvariantInfo),
                        IsolutionId = dr[4].ToString(),
                        EsolutionId = dr[5].ToString()
                    });
                    temp.Add(_ebSolutions);
                }
                resp.Data = temp;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception:" + e.Message + e.StackTrace);
            }
            return resp;
        }


        public GetSolutioInfoResponse Get(GetSolutioInfoRequest request)
        {
            ConnectionManager _conService = base.ResolveService<ConnectionManager>();
            string sql = string.Format("SELECT solution_name, description, date_created, esolution_id, pricing_tier, versioning  FROM eb_solutions WHERE isolution_id='{0}'", request.IsolutionId);
            EbDataTable dt = (new EbConnectionFactory(CoreConstants.EXPRESSBASE, this.Redis)).DataDB.DoQuery(sql);
            EbSolutionsWrapper _ebSolutions = new EbSolutionsWrapper
            {
                SolutionName = dt.Rows[0][0].ToString(),
                Description = dt.Rows[0][1].ToString(),
                DateCreated = dt.Rows[0][2].ToString(),
                EsolutionId = dt.Rows[0][3].ToString(),
                PricingTier = (PricingTiers)Convert.ToInt32(dt.Rows[0][4]),
                IsVersioningEnabled = (bool)dt.Rows[0][5]
            };
            GetSolutioInfoResponse resp = new GetSolutioInfoResponse() { Data = _ebSolutions };
            if (resp.Data != null)
            {
                GetConnectionsResponse response = (GetConnectionsResponse)_conService.Post(new GetConnectionsRequest { ConnectionType = 0, SolutionId = request.IsolutionId });
                resp.EBSolutionConnections = response.EBSolutionConnections;
            }
            return resp;
        }

        public EmailverifyResponse Post(EmailverifyRequest request)
        {
            EmailverifyResponse re = new EmailverifyResponse();
            try
            {
                string qur = String.Format(@"UPDATE 
										eb_tenants 
										SET
											is_verified = 'T',
											activation_code=null,
                                            mail_verify_time=NOW()
										WHERE 
											id = :id AND
											activation_code= :codes");

                DbParameter[] parameters = {
                    InfraConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
                    InfraConnectionFactory.ObjectsDB.GetNewParameter("codes", EbDbTypes.String, request.ActvCode)
            };
                int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(qur, parameters);

                if (dt == 1)
                {
                    re.VerifyStatus = true;
                }
                else
                {
                    re.VerifyStatus = false;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }

            return re;
        }

        public SocialAutoSignInResponse Post(SocialAutoSignInRequest Request)
        {
            SocialAutoSignInResponse respo = new SocialAutoSignInResponse();
            try
            {
                string sql = @"SELECT 
								id,
								pwd 
								FROM public.eb_tenants 
								where
								(fb_id=:soc_id or github_id=:soc_id or twitter_id=:soc_id or google_id=:soc_id) 
								and 
								email=:mail;";

                DbParameter[] parameters = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("mail", EbDbTypes.String, Request.Email),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("soc_id", EbDbTypes.String, Request.Social_id),
                    };

                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                respo.Id = Convert.ToInt32(dt.Rows[0][0]);
                respo.psw = Convert.ToString(dt.Rows[0][1]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);

            }

            return respo;
        }

		public SocialLoginResponse Post(SocialLoginRequest reqt)
		{
			Console.WriteLine("reached service / SocialLoginRequest");
			SocialLoginResponse Soclg = new SocialLoginResponse();
			SocialSignup sco_signup = new SocialSignup();
			bool unique = false;
			string pasword = null;
			try
			{
				string sql1 = "SELECT id,fb_id,github_id,twitter_id,google_id FROM eb_tenants WHERE email ~* @email and eb_del='F'";
				DbParameter[] parameters2 = { InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, reqt.Email) };
				EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql1, parameters2);
				if (dt.Rows.Count > 0)
				{
					unique = false;
					sco_signup.FbId = Convert.ToString(dt.Rows[0][1]);
					sco_signup.GithubId = Convert.ToString(dt.Rows[0][2]);
					sco_signup.TwitterId = Convert.ToString(dt.Rows[0][3]);
					sco_signup.GoogleId = Convert.ToString(dt.Rows[0][4]);
					Console.WriteLine("mail id is not unique");
					//if (urllink.Contains(pathsignup, StringComparison.OrdinalIgnoreCase))
					//{
					//	sco_signup.Forsignup = true;
					//}
					//else
					//if(urllink.Contains(pathsignin, StringComparison.OrdinalIgnoreCase))
					{
						sco_signup.Forsignup = false;

					}
				}
				else
				{
					unique = true;
				}

				if (unique == true)
				{
					string pd = Guid.NewGuid().ToString();
					if (!string.IsNullOrEmpty(reqt.Fbid) )
					{
						pasword = (reqt.Fbid + pd + reqt.Email).ToMD5Hash();
						DbParameter[] parameter1 = {
								InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, reqt.Email),
								InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  reqt.Name),
								 InfraConnectionFactory.DataDB.GetNewParameter("fbid", EbDbTypes.String,  reqt.Fbid),
								 InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword),
								 InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String,'F'),

                                 };

						EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants 
								(email,fullname,fb_id,pwd, eb_created_at,eb_del, is_verified, is_email_sent) 
                                 VALUES 
                                 (:email,:name,:fbid,:password,NOW(),:fals,:fals,:fals) RETURNING id;", parameter1);

						Console.WriteLine("inserted details to tenant table");
						sco_signup.Pauto = pasword;
					}
					else if (!string.IsNullOrEmpty(reqt.Goglid))
					{

						pasword = (reqt.Fbid + pd + reqt.Email).ToMD5Hash();
						DbParameter[] parameter1 = {
								InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, reqt.Email),
								InfraConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  reqt.Name),
								 InfraConnectionFactory.DataDB.GetNewParameter("gogl_id", EbDbTypes.String,  reqt.Goglid),
								 InfraConnectionFactory.DataDB.GetNewParameter("password", EbDbTypes.String,pasword),
								 InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String,'F'),

								 };

						EbDataTable dtbl = InfraConnectionFactory.DataDB.DoQuery(@"INSERT INTO eb_tenants 
								(email,fullname,google_id,pwd, eb_created_at,eb_del, is_verified, is_email_sent) 
                                 VALUES 
                                 (:email,:name,:gogl_id,:password,NOW(),:fals,:fals,:fals) RETURNING id;", parameter1);

						Console.WriteLine("inserted details to tenant table");
						sco_signup.Pauto = pasword;
					}
				}
				
				{
					if (!string.IsNullOrEmpty(reqt.Fbid))
					{

						sco_signup.AuthProvider = "facebook";
						sco_signup.Social_id = reqt.Fbid;
					}
					else if (!string.IsNullOrEmpty(reqt.Goglid))
					{
						sco_signup.AuthProvider = "google";
						sco_signup.Social_id = reqt.Goglid;
					}
				sco_signup.Email = reqt.Email;
				sco_signup.Fullname = reqt.Name;
				sco_signup.UniqueEmail = unique;
				};

				Soclg.jsonval = JsonConvert.SerializeObject(sco_signup);
				

            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }


			return Soclg;
		}




        public ForgotPasswordResponse Post(ForgotPasswordRequest reques)
        {
            ForgotPasswordResponse re = new ForgotPasswordResponse();
            try
            {
                string k = String.Format(@"UPDATE 
										eb_tenants 
										SET
											resetpsw_code = :code
										WHERE 
											email=:mail
                                            and eb_del='F'"
                                            );
                DbParameter[] parameters = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("code", EbDbTypes.String, reques.Resetcode),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("mail", EbDbTypes.String, reques.Email)
                    };
                int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);

                if (dt == 1)
                {
                    string pq = String.Format(@"select fullname from
										eb_tenants 
										WHERE 
											email=:mail
                                            and eb_del='F'"
                                            );
                    DbParameter[] parameters11 = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("mail", EbDbTypes.String, reques.Email)
                    };
                    EbDataTable dt11 = InfraConnectionFactory.DataDB.DoQuery(pq, parameters11);
                    //uin=unique identification number
                    //uic=unique identification code
                    string aq = "$" + reques.Email + "$" + reques.Resetcode + "$";
                    byte[] plaintxt = System.Text.Encoding.UTF8.GetBytes(aq);
                    string ai = System.Convert.ToBase64String(plaintxt);
                    string resetlink = string.Format("https://{0}/resetpassword?rep={1}", reques.PageUrl, ai);

                    //using (StreamReader reader = new StreamReader("\\Ext\\EmailVerifyStructure.cshtml")) 
                    //{
                    //	body = reader.ReadToEnd();
                    //}

                    string body = @"</head>
<body>
    <div style='border: 1px solid #508bf9;padding:20px 40px 20px 40px; '>
        <figure style='text-align: center;margin:0px;'>
            <img src='https://expressbase.com/images/logos/EB_Logo.png' /><br />
        </figure>
        <br />
        <h3 style='color:#508bf9;margin:0px'>Build Business Apps 10x faster!</h3> <br />
        <div style='line-height: 1.4;'>
            Dear {UserName},<br />
            <br />
			
			You can use the following link to reset your password:
        </div>
        <br />
        <table>
            <tr>
                <td class='btn-read-online' style='text-align: center; background-color: #508bf9; padding: 10px 15px; border-radius: 5px;'>
                    <a href='{Url}' style='color: #fff; font-size: 16px; letter-spacing: 1px; text-decoration: none;  font-family: Montserrat,Arial, Helvetica, sans-serif;'>Reset password</a>
                </td>
            </tr>
        </table>
        <br />
		If the previous button does not work, try to copy and paste the following URL in your browser’s address bar:<br />
        <a href='{Url}'>{Url}</a> 
		<br />
		<br />
        Need help? Please drop in a mail to <a href='{supporturl}'>support@expressbase.com</a>. We're right here for you.<br /><br />
        Sincerely,<br />
        Team EXPRESSbase<br />
    </div>
</body>
</html>";
                    string supporturl = "mailto:support@expressbase.com";
                    body = body.Replace("{UserName}", (dt11.Rows[0][0]).ToString());
                    body = body.Replace("{Url}", resetlink).Replace("{supporturl}", supporturl);


                    //StringBuilder bodyMsg = new StringBuilder();
                    //bodyMsg.Append( " <img src = "+ "https://expressbase.com/images/logos/EB_Logo.png" + " />");
                    //bodyMsg.Append("<p style="+"color: red;"+"><b>Please follow this link to reset your password: <b></p>");
                    //            bodyMsg.Append("<br />");
                    //            bodyMsg.Append("next3");
                    //            bodyMsg.Append("<a href=https://" + resetlink + ">Account</a>");
                    //            bodyMsg.Append("<br />");
                    //            bodyMsg.Append("next4");

                    MessageProducer3.Publish(new EmailServicesRequest
                    {
                        To = reques.Email,
                        Subject = "Reset password",
                        Message = body,
                        //Message = bodyMsg.ToString(),
                        SolnId = CoreConstants.EXPRESSBASE,

                    });
                    re.VerifyStatus = true;
                }
                else
                {
                    re.VerifyStatus = false;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }

            return re;
        }
        public ResetPasswordResponse Post(ResetPasswordRequest reqst)
        {
            ResetPasswordResponse rests = new ResetPasswordResponse();
            try
            {
                string hshpassword = (reqst.Password + reqst.Email).ToMD5Hash();
                string qur = String.Format(@"UPDATE 
										eb_tenants 
										SET
											pwd = :pswrd,
											resetpsw_code=null 
										WHERE 
											email = :id AND
											resetpsw_code= :codes");

                DbParameter[] parameters = {
                InfraConnectionFactory.ObjectsDB.GetNewParameter("pswrd",EbDbTypes.String,hshpassword),
                    InfraConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.String, reqst.Email),
                    InfraConnectionFactory.ObjectsDB.GetNewParameter("codes", EbDbTypes.String, reqst.Resetcode)
            };
                int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(qur, parameters);
                if (dt == 1)
                {

                    rests.VerifyStatus = true;
                }
                else
                {
                    rests.VerifyStatus = false;
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
            }

            return rests;
        }
        //public EditAccountResponse Post(EditAccountRequest request)
        //{
        //    EditAccountResponse resp;
        //    using (var con = EbConnectionFactory.DataDB.GetNewConnection())
        //    {
        //        Dictionary<string, object> dict = new Dictionary<string, object>();
        //        string sql = string.Format("SELECT * FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]);
        //        var dt = EbConnectionFactory.DataDB.DoQuery(sql);
        //        foreach (EbDataRow dr in dt.Rows)
        //        {
        //            foreach (EbDataColumn dc in dt.Columns)
        //            {
        //                dict.Add(dc.ColumnName, dr[dc.ColumnName]);
        //            }
        //        }
        //        resp = new EditAccountResponse()
        //        {
        //            Data = dict
        //        };
        //    }
        //    return resp;
        //}

        //public void Get(CreateApplicationRequest request)
        //{
        //    string sql =string.Format(@"SELECT A.socialid, A.id, A.fullname, A.email, A.phoneno, A.firstvisit, A.lastvisit, A.totalvisits, 
        //                        A.city, A.region, A.country, B.applicationname , concat(A.latitude::text,',' ,A.longitude::text) AS latlong
        //FROM eb_usersanonymous A, eb_applications B WHERE A.appid = B.id AND A.ebuserid = 1 AND A.appid = {0}", request.appid);

        //    var dsobj = new EbDataSource();
        //    dsobj.Sql = sql;
        //    var ds = new EbObject_Create_New_ObjectRequest();
        //    ds.Name = request.Description + "_datasource";
        //    ds.Description = "desc";
        //    ds.Json = EbSerializers.Json_Serialize(dsobj);
        //    ds.Status = ObjectLifeCycleStatus.Live;
        //    ds.Relations = "";
        //    ds.IsSave = false;
        //    ds.Tags = "";
        //    ds.Apps = request.AppName.ToString();
        //    ds.TenantAccountId = request.TenantAccountId; 
        //    //ds.WhichConsole = request.WhichConsole;
        //    ds.UserId = request.UserId;
        //    var myService = base.ResolveService<EbObjectService>();
        //    var res = myService.Post(ds);
        //    var refid = res.RefId;

        //    var dvobj = new EbTableVisualization();
        //    dvobj.DataSourceRefId = refid;
        //    //dvobj.Columns = Columns;
        //    //dvobj.DSColumns = Columns;
        //    var ds1 = new EbObject_Create_New_ObjectRequest();
        //    ds1.Name = request.Description+ "_response";
        //    ds1.Description = "desc";
        //    ds1.Json = EbSerializers.Json_Serialize(dvobj);
        //    ds1.Status = ObjectLifeCycleStatus.Live;
        //    ds1.Relations = refid;
        //    ds1.IsSave = false;
        //    ds1.Tags = "";
        //    ds1.Apps = request.AppName.ToString();
        //    ds1.TenantAccountId = request.TenantAccountId;
        //    //ds1.WhichConsole = request.WhichConsole;
        //    ds1.UserId = request.UserId;
        //    var res1 = myService.Post(ds1);
        //    var refid1 = res.RefId;
        //}

        //public DataBaseConfigResponse Post(DataBaseConfigRequest request)
        //{
        //    DataBaseConfigResponse resp = new DataBaseConfigResponse();
        //    using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //    {

        //            int uid = 0;
        //            string sql = string.Format("SELECT cid,accountname,tenantid FROM eb_tenantaccount WHERE id={0}", request.Colvalues["acid"]);
        //            var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);


        //            //CREATE CLIENTDB CONN
        //            EbClientConf e = new EbClientConf()
        //            {
        //                ClientID = dt.Rows[0][0].ToString(),
        //                ClientName = dt.Rows[0][1].ToString(),
        //                EbClientTier = EbClientTiers.Unlimited
        //            };

        //            if (request.Colvalues.ContainsKey("dbtype") && request.Colvalues["dbtype"].ToString() == "2")
        //            {
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objrw"].ToString(), request.Colvalues["sip_objrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objrw"]), request.Colvalues["duname_objrw"].ToString(), request.Colvalues["pwd_objrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_objrw"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_datarw"].ToString(), request.Colvalues["sip_datarw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_datarw"]), request.Colvalues["duname_datarw"].ToString(), request.Colvalues["pwd_datarw"].ToString(), Convert.ToInt32(request.Colvalues["tout_datarw"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_filerw"])), request.Colvalues["dbname_filerw"].ToString(), request.Colvalues["sip_filerw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_filerw"]), request.Colvalues["duname_filerw"].ToString(), request.Colvalues["pwd_filerw"].ToString(), Convert.ToInt32(request.Colvalues["tout_filerw"])));
        //                //e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_logrw"])), request.Colvalues["dbname_logrw"].ToString(), request.Colvalues["sip_logrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_logrw"]), request.Colvalues["duname_logrw"].ToString(), request.Colvalues["pwd_logrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_logrw"])));
        //                //e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objro"].ToString(), request.Colvalues["sip_objro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objro"]), request.Colvalues["duname_objro"].ToString(), request.Colvalues["pwd_objro"].ToString(), Convert.ToInt32(request.Colvalues["tout_objro"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_dataro"].ToString(), request.Colvalues["sip_dataro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_dataro"]), request.Colvalues["duname_dataro"].ToString(), request.Colvalues["pwd_dataro"].ToString(), Convert.ToInt32(request.Colvalues["tout_dataro"])));
        //            }
        //            else
        //            {
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //               // e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //              //  e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //            }


        //            byte[] bytea2 = EbSerializers.ProtoBuf_Serialize(e);
        //            var dbconf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea2);
        //            var dbf = new TenantDbFactory(dbconf);
        //            DbTransaction _con_d1_trans = null;
        //            DbTransaction _con_o1_trans = null;

        //            var _con_d1 = dbf.DataDB.GetNewConnection();
        //            var _con_d2 = dbf.DataDBRO.GetNewConnection();
        //            var _con_o1 = dbf.ObjectsDB.GetNewConnection();
        //            var _con_f1 = dbf.FilesDB.GetNewConnection();
        //            int i = 0;
        //            try
        //            {
        //                _con_d1.Open();
        //                i++;
        //                _con_d2.Open(); _con_d2.Close();
        //                i++;
        //                _con_o1.Open();
        //                i++;
        //                _con_f1.Open(); _con_f1.Close();

        //                _con_d1_trans = _con_d1.BeginTransaction();
        //                _con_o1_trans = _con_o1.BeginTransaction();

        //            }
        //            catch (Exception ex)
        //            {
        //                if (i == 0)
        //                    throw HttpError.NotFound("Error in data");

        //                else if (i == 1)
        //                    throw HttpError.NotFound("Error in data read only");

        //                else if (i == 2)
        //                    throw HttpError.NotFound("Error in objects");

        //                else if (i == 3)
        //                    throw HttpError.NotFound("Error in objects read only");

        //                else if (i == 4)
        //                    throw HttpError.NotFound("Error in logs");

        //                else if (i == 5)
        //                    throw HttpError.NotFound("Error in log read only");

        //                else if (i == 6)
        //                    throw HttpError.NotFound("Error in files");

        //                else if (i == 7)
        //                    throw HttpError.NotFound("Error in files reda only");

        //                else
        //                    throw HttpError.NotFound("Success");
        //            }

        //            var tenantdt = InfraDatabaseFactory.InfraDB.DoQuery(string.Format("SELECT cname,firstname,phone,password FROM eb_tenants WHERE id={0}", dt.Rows[0][2]));
        //            try
        //            {
        //                TableInsertsDataDB(dbf, tenantdt, _con_d1);
        //                TableInsertObjectDB(dbf, _con_o1);
        //                _con_d1_trans.Commit();
        //                _con_o1_trans.Commit();
        //                _con_d1.Close();
        //                _con_o1.Close();

        //            }
        //            catch (Exception ex)
        //            {
        //                string error = null;

        //                if (_con_d1.State == System.Data.ConnectionState.Open)// || _con_o1.State == System.Data.ConnectionState.Open)
        //                    error = "Database for data is already in use.Please connect a new database";
        //                else if (_con_o1.State == System.Data.ConnectionState.Open)
        //                    error = "Database for objects is already in use.Please connect a new database";

        //                _con_d1_trans.Rollback();
        //                _con_o1_trans.Rollback();
        //                throw HttpError.NotFound(error);
        //            }

        //            var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenantaccount SET config=@config,dbconfigtype=@dbconfigtype WHERE id=@id RETURNING id");
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("config", System.Data.DbType.Binary, bytea2));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("dbconfigtype", System.Data.DbType.Int32, request.Colvalues["dbtype"]));
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, Convert.ToInt32(request.Colvalues["acid"])));
        //            uid = Convert.ToInt32(cmd.ExecuteScalar());
        //            resp.id = uid;


        //        }
        //    return resp;
        //}

        //public EditDBConfigResponse Post(EditDBConfigRequest request)
        //{
        //    EditDBConfigResponse resp;
        //    using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //    {
        //        Dictionary<string, object> dbresults = new Dictionary<string, object>();
        //        var dt = InfraDatabaseFactory.InfraDB.DoQuery(string.Format("SELECT dbconfigtype,config FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]));
        //        int db_conf_type = 0;

        //        db_conf_type = Convert.ToInt32(dt.Rows[0][0]);

        //        byte[] bytea = (dt.Rows[0][1] != DBNull.Value) ? (byte[])dt.Rows[0][1] : null;

        //        if (bytea != null)
        //        {
        //            EbClientConf conf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea);
        //            dbresults[Constants.CONF_DBTYPE] = db_conf_type;
        //            if (db_conf_type == 1)
        //            {

        //                dbresults[Constants.CONF_VENDOR] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseVendor;
        //                dbresults[Constants.CONF_DBNAME] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseName;
        //                dbresults[Constants.CONF_SIP] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Server;
        //                dbresults[Constants.CONF_PORT] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Port;
        //                dbresults[Constants.CONF_UNAME] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].UserName;
        //                dbresults[Constants.CONF_TOUT] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Timeout;
        //                dbresults[Constants.CONF_PWD] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Password;

        //            }
        //            else
        //            {

        //                dbresults[Constants.CONF_VENDOR_DATA] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseVendor;
        //                dbresults[Constants.CONF_VENDOR_OBJ] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].DatabaseVendor;
        //                dbresults[Constants.CONF_VENDOR_FILES] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].DatabaseVendor;
        //               // dbresults[Constants.CONF_VENDOR_LOGS] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].DatabaseVendor;

        //                dbresults[Constants.CONF_DBNAME_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseName;
        //                dbresults[Constants.CONF_DBNAME_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].DatabaseName;
        //                dbresults[Constants.CONF_DBNAME_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].DatabaseName;
        //               // dbresults[Constants.CONF_DBNAME_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].DatabaseName;
        //                dbresults[Constants.CONF_DBNAME_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].DatabaseName;
        //               // dbresults[Constants.CONF_DBNAME_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].DatabaseName;

        //                dbresults[Constants.CONF_SIP_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Server;
        //                dbresults[Constants.CONF_SIP_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Server;
        //                dbresults[Constants.CONF_SIP_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Server;
        //                //dbresults[Constants.CONF_SIP_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Server;
        //                dbresults[Constants.CONF_SIP_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Server;
        //                //dbresults[Constants.CONF_SIP_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Server;

        //                dbresults[Constants.CONF_PORT_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Port;
        //                dbresults[Constants.CONF_PORT_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Port;
        //                dbresults[Constants.CONF_PORT_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Port;
        //                //dbresults[Constants.CONF_PORT_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Port;
        //                dbresults[Constants.CONF_PORT_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Port;
        //                //dbresults[Constants.CONF_PORT_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Port;

        //                dbresults[Constants.CONF_UNAME_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].UserName;
        //                dbresults[Constants.CONF_UNAME_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].UserName;
        //                dbresults[Constants.CONF_UNAME_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].UserName;
        //                //dbresults[Constants.CONF_UNAME_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].UserName;
        //                dbresults[Constants.CONF_UNAME_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].UserName;
        //                //dbresults[Constants.CONF_UNAME_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].UserName;

        //                dbresults[Constants.CONF_TOUT_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Timeout;
        //                dbresults[Constants.CONF_TOUT_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Timeout;
        //                dbresults[Constants.CONF_TOUT_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Timeout;
        //                //dbresults[Constants.CONF_TOUT_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Timeout;
        //                dbresults[Constants.CONF_TOUT_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Timeout;
        //                //dbresults[Constants.CONF_TOUT_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Timeout;

        //                dbresults[Constants.CONF_PWD_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Password;
        //                dbresults[Constants.CONF_PWD_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Password;
        //                dbresults[Constants.CONF_PWD_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Password;
        //                //dbresults[Constants.CONF_PWD_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Password;
        //                dbresults[Constants.CONF_PWD_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Password;
        //                //dbresults[Constants.CONF_PWD_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Password;
        //            }
        //            resp = new EditDBConfigResponse
        //            {
        //                Data = dbresults
        //            };
        //        }
        //        else
        //        {
        //            resp = new EditDBConfigResponse
        //            {
        //                Data = dbresults
        //            };
        //        }
        //    }
        //    return resp;
        //}

        //public TokenRequiredUploadResponse Any(TokenRequiredUploadRequest request)
        //{
        //    TokenRequiredUploadResponse resp = null;

        //    ILog log = LogManager.GetLogger(GetType());

        //    if (request.TenantAccountId != CoreConstants.EXPRESSBASE)
        //    {
        //        //base.ClientID = request.TenantAccountId;
        //        using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
        //        {
        //            con.Open();
        //            if (request.op == "createuser")
        //            {
        //                string sql = "INSERT INTO eb_users (firstname,email,pwd) VALUES (@firstname,@email,@pwd) RETURNING id,pwd;";
        //                sql += "INSERT INTO eb_role2user (role_id,user_id) SELECT id, (CURRVAL('eb_users_id_seq')) FROM UNNEST(@roles) AS id";
        //                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["firstname"]),
        //                    this.TenantDbFactory.ObjectsDB.GetNewParameter("email", System.Data.DbType.String, request.Colvalues["email"]),
        //                    this.TenantDbFactory.ObjectsDB.GetNewParameter("roles", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, request.Colvalues["roles"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray()),
        //                    this.TenantDbFactory.ObjectsDB.GetNewParameter("pwd", System.Data.DbType.String,string.IsNullOrEmpty(request.Colvalues["pwd"].ToString())? GeneratePassword() :request.Colvalues["pwd"] )};

        //                EbDataSet dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

        //                if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()))
        //                {
        //                    using (var service = base.ResolveService<EmailServices>())
        //                    {
        //                        service.Any(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
        //                    }
        //                }



        //            }
        //            else if (request.op == "rbac_roles")
        //            {
        //                string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @users, @dependants,@permission );";
        //                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
        //                int[] emptyarr = new int[] { };
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_id", System.Data.DbType.Int32, request.Colvalues["roleid"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["Description"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_name", System.Data.DbType.String, request.Colvalues["role_name"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationid", System.Data.DbType.Int32, request.Colvalues["applicationid"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("createdby", System.Data.DbType.Int32, request.UserId));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("permission", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => n.ToString()).ToArray() : new string[] { }));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("users", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("dependants", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));

        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = Convert.ToInt32(cmd.ExecuteScalar())

        //                };
        //            }

        //            else if (request.op == "usergroups")
        //            {
        //                string sql = "";
        //                if (request.Id > 0)
        //                {
        //                    sql = @"UPDATE eb_usergroup SET name = @name,description = @description WHERE id = @id;
        //                            INSERT INTO eb_user2usergroup(userid,groupid) SELECT uid,@id FROM UNNEST(array(SELECT unnest(@users) except 
        //                                SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')))) as uid;
        //                            UPDATE eb_user2usergroup SET eb_del = 'T' WHERE userid IN(
        //                                SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')) except SELECT UNNEST(@users));";
        //                }
        //                else
        //                {
        //                    sql = @"INSERT INTO eb_usergroup (name,description) VALUES (@name,@description) RETURNING id;
        //                               INSERT INTO eb_user2usergroup (userid,groupid) SELECT id, (CURRVAL('eb_usergroup_id_seq')) FROM UNNEST(@users) AS id";
        //                }

        //                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
        //                int[] emptyarr = new int[] { };
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("name", System.Data.DbType.String, request.Colvalues["groupname"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["description"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("users", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["userlist"].ToString() != string.Empty) ? request.Colvalues["userlist"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.Id));
        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = Convert.ToInt32(cmd.ExecuteScalar())

        //                };
        //            }
        //            else
        //            {
        //                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_users SET locale=@locale,timezone=@timezone,dateformat=@dateformat,numformat=@numformat,timezonefull=@timezonefull WHERE id=@id");
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("locale", System.Data.DbType.String, request.Colvalues["locale"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("timezone", System.Data.DbType.String, request.Colvalues["timecode"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("dateformat", System.Data.DbType.String, request.Colvalues["dateformat"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("numformat", System.Data.DbType.String, request.Colvalues["numformat"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("timezonefull", System.Data.DbType.String, request.Colvalues["timezone"]));
        //                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["uid"]));
        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = Convert.ToInt32(cmd.ExecuteScalar())

        //                };
        //            }

        //        }
        //    }
        //    else

        //        using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //        {
        //            con.Open();
        //            log.Info("#Eb account insert 1");
        //            if (request.Colvalues.ContainsKey("op") && request.Colvalues["op"].ToString() == "insertaccount")
        //            {
        //                var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "INSERT INTO eb_tenantaccount (accountname,cid,address,phone,email,website,tier,tenantname,createdat,validtill,profilelogo,tenantid)VALUES(@accountname,@cid,@address,@phone,@email,@website,@tier,@tenantname,now(),(now()+ interval '30' day),@profilelogo,@tenantid) ON CONFLICT(cid) DO UPDATE SET accountname=@accountname,address=@address,phone=@phone,email=@email,website=@website,tier=@tier,createdat=now(),validtill=(now()+ interval '30' day) RETURNING id ");
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("accountname", System.Data.DbType.String, request.Colvalues["accountname"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("cid", System.Data.DbType.String, request.Colvalues["cid"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("address", System.Data.DbType.String, request.Colvalues["address"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["phone"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("email", System.Data.DbType.String, request.Colvalues["email"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("website", System.Data.DbType.String, request.Colvalues["website"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tier", System.Data.DbType.String, request.Colvalues["tier"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantname", System.Data.DbType.String, request.Colvalues["tenantname"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profilelogo", System.Data.DbType.String, string.Format("<img src='{0}'/>", request.Colvalues["imgpro"])));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("tenantid", System.Data.DbType.Int64, request.Colvalues["tenantid"]));
        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = Convert.ToInt32(cmd.ExecuteScalar())
        //                };
        //                base.Redis.Set<string>(string.Format("cid_{0}_uid_{1}_pimg", request.Colvalues["cid"], resp.id), string.Format("<img src='{0}'class='img-circle img-cir'/>", request.Colvalues["imgpro"]));

        //            }
        //            else if (request.Colvalues.ContainsKey("dbcheck") && request.Colvalues["dbcheck"].ToString() == "dbconfig")
        //            {
        //                resp = new TokenRequiredUploadResponse();
        //                int uid = 0;
        //                string sql = string.Format("SELECT cid,accountname,tenantid FROM eb_tenantaccount WHERE id={0}", request.Colvalues["acid"]);
        //                var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);


        //                //CREATE CLIENTDB CONN
        //                EbClientConf e = new EbClientConf()
        //                {
        //                    ClientID = dt.Rows[0][0].ToString(),
        //                    ClientName = dt.Rows[0][1].ToString(),
        //                    EbClientTier = EbClientTiers.Unlimited
        //                };

        //                if (request.Colvalues.ContainsKey("dbtype") && request.Colvalues["dbtype"].ToString() == "2")
        //                {
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objrw"].ToString(), request.Colvalues["sip_objrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objrw"]), request.Colvalues["duname_objrw"].ToString(), request.Colvalues["pwd_objrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_objrw"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_datarw"].ToString(), request.Colvalues["sip_datarw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_datarw"]), request.Colvalues["duname_datarw"].ToString(), request.Colvalues["pwd_datarw"].ToString(), Convert.ToInt32(request.Colvalues["tout_datarw"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_filerw"])), request.Colvalues["dbname_filerw"].ToString(), request.Colvalues["sip_filerw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_filerw"]), request.Colvalues["duname_filerw"].ToString(), request.Colvalues["pwd_filerw"].ToString(), Convert.ToInt32(request.Colvalues["tout_filerw"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_logrw"])), request.Colvalues["dbname_logrw"].ToString(), request.Colvalues["sip_logrw"].ToString(), Convert.ToInt32(request.Colvalues["pnum_logrw"]), request.Colvalues["duname_logrw"].ToString(), request.Colvalues["pwd_logrw"].ToString(), Convert.ToInt32(request.Colvalues["tout_logrw"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_objrw"])), request.Colvalues["dbname_objro"].ToString(), request.Colvalues["sip_objro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_objro"]), request.Colvalues["duname_objro"].ToString(), request.Colvalues["pwd_objro"].ToString(), Convert.ToInt32(request.Colvalues["tout_objro"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_datarw"])), request.Colvalues["dbname_dataro"].ToString(), request.Colvalues["sip_dataro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_dataro"]), request.Colvalues["duname_dataro"].ToString(), request.Colvalues["pwd_dataro"].ToString(), Convert.ToInt32(request.Colvalues["tout_dataro"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_filerw"])), request.Colvalues["dbname_filero"].ToString(), request.Colvalues["sip_filero"].ToString(), Convert.ToInt32(request.Colvalues["pnum_filero"]), request.Colvalues["duname_filero"].ToString(), request.Colvalues["pwd_filero"].ToString(), Convert.ToInt32(request.Colvalues["tout_filero"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db_logrw"])), request.Colvalues["dbname_logro"].ToString(), request.Colvalues["sip_logro"].ToString(), Convert.ToInt32(request.Colvalues["pnum_logro"]), request.Colvalues["duname_logro"].ToString(), request.Colvalues["pwd_logro"].ToString(), Convert.ToInt32(request.Colvalues["tout_logro"])));
        //                }
        //                else
        //                {
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbOBJECTS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbOBJECTS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbDATA_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbDATA_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbFILES_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbFILES_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                    e.DatabaseConfigurations.Add(EbConnectionTypes.EbLOGS_RO, new EbDatabaseConfiguration(EbConnectionTypes.EbLOGS_RO, (DatabaseVendors)(Convert.ToInt32(request.Colvalues["db"])), request.Colvalues["dbname"].ToString(), request.Colvalues["sip"].ToString(), Convert.ToInt32(request.Colvalues["pnum"]), request.Colvalues["duname"].ToString(), request.Colvalues["pwd"].ToString(), Convert.ToInt32(request.Colvalues["tout"])));
        //                }


        //                byte[] bytea2 = EbSerializers.ProtoBuf_Serialize(e);
        //                var dbconf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea2);
        //                var dbf = new DatabaseFactory(dbconf);
        //                DbTransaction _con_d1_trans = null;
        //                DbTransaction _con_o1_trans = null;

        //                var _con_d1 = dbf.DataDB.GetNewConnection();
        //                var _con_d2 = dbf.DataDBRO.GetNewConnection();
        //                var _con_o1 = dbf.ObjectsDB.GetNewConnection();
        //                var _con_o2 = dbf.ObjectsDBRO.GetNewConnection();
        //                var _con_l1 = dbf.LogsDB.GetNewConnection();
        //                var _con_l2 = dbf.LogsDBRO.GetNewConnection();
        //                var _con_f1 = dbf.FilesDB.GetNewConnection();
        //                var _con_f2 = dbf.FilesDBRO.GetNewConnection();
        //                int i = 0;
        //                try
        //                {
        //                    _con_d1.Open();
        //                    i++;
        //                    _con_d2.Open(); _con_d2.Close();
        //                    i++;
        //                    _con_o1.Open();
        //                    i++;
        //                    _con_o2.Open(); _con_o2.Close();
        //                    i++;
        //                    _con_l1.Open(); _con_l1.Close();
        //                    i++;
        //                    _con_l2.Open(); _con_l2.Close();
        //                    i++;
        //                    _con_f1.Open(); _con_f1.Close();
        //                    i++;
        //                    _con_f2.Open(); _con_f2.Close();

        //                    _con_d1_trans = _con_d1.BeginTransaction();
        //                    _con_o1_trans = _con_o1.BeginTransaction();

        //                }
        //                catch (Exception ex)
        //                {
        //                    if (i == 0)
        //                        throw HttpError.NotFound("Error in data");

        //                    else if (i == 1)
        //                        throw HttpError.NotFound("Error in data read only");

        //                    else if (i == 2)
        //                        throw HttpError.NotFound("Error in objects");

        //                    else if (i == 3)
        //                        throw HttpError.NotFound("Error in objects read only");

        //                    else if (i == 4)
        //                        throw HttpError.NotFound("Error in logs");

        //                    else if (i == 5)
        //                        throw HttpError.NotFound("Error in log read only");

        //                    else if (i == 6)
        //                        throw HttpError.NotFound("Error in files");

        //                    else if (i == 7)
        //                        throw HttpError.NotFound("Error in files reda only");

        //                    else
        //                        throw HttpError.NotFound("Success");
        //                }

        //                var tenantdt = InfraDatabaseFactory.InfraDB.DoQuery(string.Format("SELECT cname,firstname,phone,password FROM eb_tenants WHERE id={0}", dt.Rows[0][2]));
        //                try
        //                {
        //                    TableInsertsDataDB(dbf, tenantdt, _con_d1);
        //                    TableInsertObjectDB(dbf, _con_o1);
        //                    _con_d1_trans.Commit();
        //                    _con_o1_trans.Commit();
        //                    _con_d1.Close();
        //                    _con_o1.Close();

        //                }
        //                catch (Exception ex)
        //                {
        //                    string error = null;

        //                    if (_con_d1.State == System.Data.ConnectionState.Open)// || _con_o1.State == System.Data.ConnectionState.Open)
        //                        error = "Database for data is already in use.Please connect a new database";
        //                    else if (_con_o1.State == System.Data.ConnectionState.Open)
        //                        error = "Database for objects is already in use.Please connect a new database";

        //                    _con_d1_trans.Rollback();
        //                    _con_o1_trans.Rollback();
        //                    throw HttpError.NotFound(error);
        //                }



        //                var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenantaccount SET config=@config,dbconfigtype=@dbconfigtype WHERE id=@id RETURNING id");
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("config", System.Data.DbType.Binary, bytea2));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("dbconfigtype", System.Data.DbType.Int32, request.Colvalues["dbtype"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, Convert.ToInt32(request.Colvalues["acid"])));
        //                uid = Convert.ToInt32(cmd.ExecuteScalar());
        //                resp.id = uid;


        //            }


        //        else if (request.op == "updatetenant")
        //            {
        //                var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, "UPDATE eb_tenants SET firstname=@firstname,company=@company,employees=@employees,designation=@designation,phone=@phone,profileimg=@profileimg WHERE id=@id RETURNING id");

        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["Name"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("company", System.Data.DbType.String, request.Colvalues["Company"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("employees", System.Data.DbType.String, request.Colvalues["Employees"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("designation", System.Data.DbType.String, request.Colvalues["Designation"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("phone", System.Data.DbType.String, request.Colvalues["Phone"]));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("id", System.Data.DbType.Int64, request.UserId));
        //                cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter("profileimg", System.Data.DbType.String, string.Format("<img src='{0}'/>", request.Colvalues["proimg"])));
        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = Convert.ToInt32(cmd.ExecuteScalar())
        //                };

        //                //base.Redis.Set<string>(string.Format("uid_{0}_pimg", resp.id), string.Format("<img src='{0}'class='img-circle img-cir'/>", request.Colvalues["proimg"]));
        //            }

        //            else if (request.Colvalues.ContainsKey("edit") && request.Colvalues["edit"].ToString() == "edit")
        //            {
        //                Dictionary<string, object> dict = new Dictionary<string, object>();
        //                string sql = string.Format("SELECT * FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]);
        //                var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
        //                foreach (EbDataRow dr in dt.Rows)
        //                {
        //                    foreach (EbDataColumn dc in dt.Columns)
        //                    {
        //                        dict.Add(dc.ColumnName, dr[dc.ColumnName]);
        //                    }
        //                }
        //                resp = new TokenRequiredUploadResponse()
        //                {
        //                    Data = dict
        //                };
        //            }
        //            else if (request.Colvalues.ContainsKey("dbedit") && request.Colvalues["dbedit"].ToString() == "dbedit")
        //            {
        //                Dictionary<string, object> dbresults = new Dictionary<string, object>();
        //                var dt = InfraDatabaseFactory.InfraDB_RO.DoQuery(string.Format("SELECT dbconfigtype,config FROM eb_tenantaccount WHERE id={0}", request.Colvalues["id"]));
        //                int db_conf_type = 0;

        //                db_conf_type = Convert.ToInt32(dt.Rows[0][0]);

        //                byte[] bytea = (dt.Rows[0][1] != DBNull.Value) ? (byte[])dt.Rows[0][1] : null;

        //                if (bytea != null)
        //                {
        //                    EbClientConf conf = EbSerializers.ProtoBuf_DeSerialize<EbClientConf>(bytea);
        //                    dbresults[Constants.CONF_DBTYPE] = db_conf_type;
        //                    if (db_conf_type == 1)
        //                    {

        //                        dbresults[Constants.CONF_VENDOR] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseVendor;
        //                        dbresults[Constants.CONF_DBNAME] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseName;
        //                        dbresults[Constants.CONF_SIP] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Server;
        //                        dbresults[Constants.CONF_PORT] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Port;
        //                        dbresults[Constants.CONF_UNAME] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].UserName;
        //                        dbresults[Constants.CONF_TOUT] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Timeout;
        //                        dbresults[Constants.CONF_PWD] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Password;

        //                    }
        //                    else
        //                    {

        //                        dbresults[Constants.CONF_VENDOR_DATA] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseVendor;
        //                        dbresults[Constants.CONF_VENDOR_OBJ] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].DatabaseVendor;
        //                        dbresults[Constants.CONF_VENDOR_FILES] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].DatabaseVendor;
        //                        dbresults[Constants.CONF_VENDOR_LOGS] = (int)conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].DatabaseVendor;

        //                        dbresults[Constants.CONF_DBNAME_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].DatabaseName;
        //                        dbresults[Constants.CONF_DBNAME_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].DatabaseName;

        //                        dbresults[Constants.CONF_SIP_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Server;
        //                        dbresults[Constants.CONF_SIP_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Server;
        //                        dbresults[Constants.CONF_SIP_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Server;
        //                        dbresults[Constants.CONF_SIP_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Server;
        //                        dbresults[Constants.CONF_SIP_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Server;
        //                        dbresults[Constants.CONF_SIP_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Server;
        //                        dbresults[Constants.CONF_SIP_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].Server;
        //                        dbresults[Constants.CONF_SIP_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].Server;

        //                        dbresults[Constants.CONF_PORT_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Port;
        //                        dbresults[Constants.CONF_PORT_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Port;
        //                        dbresults[Constants.CONF_PORT_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Port;
        //                        dbresults[Constants.CONF_PORT_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Port;
        //                        dbresults[Constants.CONF_PORT_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Port;
        //                        dbresults[Constants.CONF_PORT_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Port;
        //                        dbresults[Constants.CONF_PORT_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].Port;
        //                        dbresults[Constants.CONF_PORT_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].Port;

        //                        dbresults[Constants.CONF_UNAME_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].UserName;
        //                        dbresults[Constants.CONF_UNAME_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].UserName;
        //                        dbresults[Constants.CONF_UNAME_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].UserName;
        //                        dbresults[Constants.CONF_UNAME_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].UserName;
        //                        dbresults[Constants.CONF_UNAME_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].UserName;
        //                        dbresults[Constants.CONF_UNAME_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].UserName;
        //                        dbresults[Constants.CONF_UNAME_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].UserName;
        //                        dbresults[Constants.CONF_UNAME_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].UserName;

        //                        dbresults[Constants.CONF_TOUT_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Timeout;
        //                        dbresults[Constants.CONF_TOUT_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Timeout;
        //                        dbresults[Constants.CONF_TOUT_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Timeout;
        //                        dbresults[Constants.CONF_TOUT_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Timeout;
        //                        dbresults[Constants.CONF_TOUT_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Timeout;
        //                        dbresults[Constants.CONF_TOUT_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Timeout;
        //                        dbresults[Constants.CONF_TOUT_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].Timeout;
        //                        dbresults[Constants.CONF_TOUT_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].Timeout;

        //                        dbresults[Constants.CONF_PWD_DATA_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA].Password;
        //                        dbresults[Constants.CONF_PWD_OBJ_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS].Password;
        //                        dbresults[Constants.CONF_PWD_FILES_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES].Password;
        //                        dbresults[Constants.CONF_PWD_LOGS_RW] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS].Password;
        //                        dbresults[Constants.CONF_PWD_DATA_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbDATA_RO].Password;
        //                        dbresults[Constants.CONF_PWD_OBJ_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbOBJECTS_RO].Password;
        //                        dbresults[Constants.CONF_PWD_FILES_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbFILES_RO].Password;
        //                        dbresults[Constants.CONF_PWD_LOGS_RO] = conf.DatabaseConfigurations[EbConnectionTypes.EbLOGS_RO].Password;
        //                    }
        //                    resp = new TokenRequiredUploadResponse
        //                    {
        //                        Data = dbresults
        //                    };
        //                }
        //                else
        //                {
        //                    resp = new TokenRequiredUploadResponse
        //                    {
        //                        Data = dbresults
        //                    };
        //                }
        //            }
        //            else
        //            {
        //                resp = new TokenRequiredUploadResponse
        //                {
        //                    id = 0
        //                };
        //            }
        //        }
        //    return resp;
        //}

        public TokenRequiredSelectResponse Any(TokenRequiredSelectRequest request)
        {
            //if (!string.IsNullOrEmpty(request.TenantAccountId) && request.TenantAccountId != CoreConstants.EXPRESSBASE)
            //{
            //    using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            //    {
            //        con.Open();
            //        TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse();
            //        if (request.restype == "subroles")
            //        {
            //            string sql = string.Empty;
            //            if (request.id > 0)
            //                sql = @"
            //                       SELECT id,role_name FROM eb_roles WHERE id != @id AND applicationid= @applicationid;
            //                       SELECT role2_id FROM eb_role2role WHERE role1_id = @id AND eb_del = 'F'"; //check sql properly
            //            else
            //                sql = "SELECT id,role_name FROM eb_roles WHERE applicationid= @applicationid";

            //            DbParameter[] parameters = {
            //                this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id),
            //            this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationid", System.Data.DbType.Int32,request.Colvalues["applicationid"])};

            //            var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

            //            Dictionary<string, object> returndata = new Dictionary<string, object>();
            //            List<int> subroles = new List<int>();
            //            foreach (EbDataRow dr in dt.Tables[0].Rows)
            //            {
            //                returndata[dr[0].ToString()] = dr[1].ToString();
            //            }

            //            if (dt.Tables.Count > 1)
            //            {
            //                foreach (EbDataRow dr in dt.Tables[1].Rows)
            //                {
            //                    subroles.Add(Convert.ToInt32(dr[0]));
            //                }
            //                returndata.Add("roles", subroles);
            //            }
            //            resp.Data = returndata;
            //        }

            //        else if (request.restype == "roles")
            //        {
            //            string sql = "SELECT id,role_name FROM eb_roles";
            //            var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql);

            //            Dictionary<string, object> returndata = new Dictionary<string, object>();
            //            foreach (EbDataRow dr in dt.Tables[0].Rows)
            //            {
            //                returndata[dr[0].ToString()] = dr[1].ToString();
            //            }
            //            resp.Data = returndata;
            //        }
            //        else if (request.restype == "getpermissions")
            //        {
            //            // ROLE HIERARCHY TO BE IMPLEMENTED

            //            string sql = @"
            //    SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
            //    SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = 'F';
            //    SELECT obj_name FROM eb_objects WHERE id IN(SELECT applicationid FROM eb_roles WHERE id = @id);
            //    SELECT refid FROM eb_objects_ver WHERE eb_objects_id IN(SELECT applicationid FROM eb_roles WHERE id = @id)";



            //            DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

            //            var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);
            //            List<string> _lstPermissions = new List<string>();

            //            foreach (var dr in ds.Tables[1].Rows)
            //                _lstPermissions.Add(dr[0].ToString());

            //            resp.Permissions = _lstPermissions;
            //            Dictionary<string, object> result = new Dictionary<string, object>();
            //            foreach (var dr in ds.Tables[0].Rows)
            //            {

            //                result.Add("rolename", dr[0].ToString());
            //                result.Add("applicationid", Convert.ToInt32(dr[1]));
            //                result.Add("description", dr[2].ToString());
            //            }


            //            foreach (var dr in ds.Tables[2].Rows)
            //                result.Add("applicationname", dr[0].ToString());

            //            foreach (var dr in ds.Tables[3].Rows)
            //                result.Add("dominantrefid", dr[0].ToString());

            //            resp.Data = result;
            //        }
            //        else if (request.restype == "users")
            //        {
            //            string sql = "SELECT id,firstname FROM eb_users WHERE firstname ~* @searchtext";

            //            DbParameter[] parameters = {this.TenantDbFactory.ObjectsDB.GetNewParameter("searchtext", System.Data.DbType.String,(request.Colvalues != null)?request.Colvalues["searchtext"]:string.Empty) };

            //            var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql,parameters);

            //            Dictionary<string, object> returndata = new Dictionary<string, object>();
            //            foreach (EbDataRow dr in dt.Tables[0].Rows)
            //            {
            //                returndata[dr[0].ToString()] = dr[1].ToString();
            //            }
            //            resp.Data = returndata;
            //        }
            //        else if (request.restype == "getroleusers")
            //        {
            //            string sql = @"
            //                      SELECT id,firstname FROM eb_users WHERE id IN(SELECT user_id FROM eb_role2user WHERE role_id = @roleid AND eb_del = 'F')";


            //            DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.UserId),
            //                                        this.TenantDbFactory.ObjectsDB.GetNewParameter("roleid", System.Data.DbType.Int32, request.id)};

            //            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

            //            Dictionary<string, object> returndata = new Dictionary<string, object>();

            //            foreach (EbDataRow dr in dt.Rows)
            //            {
            //                returndata[dr[0].ToString()] = dr[1].ToString();
            //            }                      
            //            resp.Data = returndata;
            //        }
            //        else if (request.restype == "usergroup")
            //        {
            //            string sql = @"
            //                      SELECT id,name FROM eb_usergroup";


            //            var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql);

            //            Dictionary<string, object> returndata = new Dictionary<string, object>();
            //            foreach (EbDataRow dr in dt.Tables[0].Rows)
            //            {
            //                returndata[dr[0].ToString()] = dr[1].ToString();
            //            }
            //            resp.Data = returndata;
            //        }
            //        else if (request.restype == "usergroupedit")
            //        {
            //            string sql = @"
            //                      SELECT id,name,description FROM eb_usergroup WHERE id = @id;
            //                      SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')";


            //            DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

            //            var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

            //            Dictionary<string, object> result = new Dictionary<string, object>();
            //            foreach (var dr in ds.Tables[0].Rows)
            //            {

            //                result.Add("name", dr[1].ToString());
            //                result.Add("description", dr[2].ToString());
            //            }
            //            List<int> users = new List<int>();
            //            if (ds.Tables.Count > 1)
            //            {
            //                foreach (EbDataRow dr in ds.Tables[1].Rows)
            //                {
            //                    users.Add(Convert.ToInt32(dr[0]));
            //                    result.Add(dr[0].ToString(), dr[1]);
            //                }
            //                result.Add("userslist", users);
            //            }
            //            resp.Data = result;
            //        }

            //        return resp;
            //    }
            //}
            //else
            {
                using (DbConnection con = EbConnectionFactory.DataDB.GetNewConnection())
                {
                    con.Open();
                    if (request.restype == "img")
                    {
                        string sql = string.Format("SELECT id,profileimg FROM eb_tenants WHERE id={0}", request.Uid);
                        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql);
                        // Dictionary<int, string> list = new Dictionary<int, string>();
                        List<List<object>> list = new List<List<object>>();
                        foreach (EbDataRow dr in dt.Rows)
                        {
                            list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString() });
                        }
                        TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                        {
                            returnlist = list
                        };
                        return resp;
                    }
                    else
                    {
                        string sql = string.Format("SELECT id,profileimg FROM eb_tenants WHERE cname={0}", request.Uname);
                        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(sql);
                        List<List<object>> list = new List<List<object>>();
                        foreach (EbDataRow dr in dt.Rows)
                        {
                            list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString() });
                        }
                        TokenRequiredSelectResponse resp = new TokenRequiredSelectResponse()
                        {
                            returnlist = list
                        };
                        return resp;

                    }

                }
            }
        }

        public void TableInsertsDataDB(EbConnectionFactory dbf, EbDataTable dt, DbConnection _con_d1)
        {
            string result;
            Assembly assembly = typeof(sqlscripts).GetAssembly();
            using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Data.SqlScripts.PostGreSql.DataDb.postgres_eb_users.sql"))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    result = reader.ReadToEnd();
                }

                var datacmd = dbf.DataDB.GetNewCommand(_con_d1, result);
                datacmd.ExecuteNonQuery();
                var cmd = dbf.DataDB.GetNewCommand(_con_d1, "INSERT INTO eb_users(email,pwd,fullname,phnoprimary) VALUES(@email,@pwd,@fullname,@phnoprimary); INSERT INTO eb_role2user(user_id,role_id) VALUES(1,3)");
                cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, dt.Rows[0][0]));
                cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String, dt.Rows[0][3]));
                cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("fullname", EbDbTypes.String, dt.Rows[0][1]));
                cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("phnoprimary", EbDbTypes.String, dt.Rows[0][2]));
                cmd.ExecuteScalar();
            }

        }

        public void TableInsertObjectDB(EbConnectionFactory dbf, DbConnection _con_o1)
        {
            string result;
            var assembly = typeof(sqlscripts).Assembly;
            using (Stream stream = assembly.GetManifestResourceStream("ExpressBase.Data.SqlScripts.PostGreSql.ObjectsDb.postgres_eb_objects.sql"))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    result = reader.ReadToEnd();
                }
                var datacmd = dbf.ObjectsDB.GetNewCommand(_con_o1, result);
                datacmd.ExecuteNonQuery();
            }

        }

        public UniqueRequestResponse Any(UniqueRequest request)
        {
            UniqueRequestResponse res = new UniqueRequestResponse();
            ILog log = LogManager.GetLogger(GetType());
            string sql = "SELECT id, pwd FROM eb_tenants WHERE email ~* @email and eb_del='F'";
            DbParameter[] parameters = { this.InfraConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.email) };
            var dt = this.InfraConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
            if (dt.Rows.Count > 0)
            {
                res.Unique = false;
                res.Id = Convert.ToInt32(dt.Rows[0]["id"]);
                res.HasPassword = (string.IsNullOrEmpty(dt.Rows[0]["pwd"].ToString())) ? false : true;
            }
            else
                res.Unique = true;

            return res;
        }

        public GetAccountResponse Any(GetAccountRequest request)
        {
            string sql = string.Format("SELECT id,solutionname,profilelogo,solutionid,createdat FROM eb_solutions WHERE tenantid={0}", request.UserId);
            var dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
            List<List<object>> list = new List<List<object>>();
            foreach (EbDataRow dr in dt.Rows)
            {
                list.Add(new List<object> { Convert.ToInt32(dr[0]), dr[1].ToString(), dr[2].ToString(), dr[3].ToString(), dr[4] });
            }
            GetAccountResponse resp = new GetAccountResponse()
            {
                returnlist = list
            };
            return resp;
        }

        //public InfraDb_GENERIC_SELECTResponse Any(InfraDb_GENERIC_SELECTRequest req)
        //{
        //    using (var con = InfraDatabaseFactory.InfraDB.GetNewConnection())
        //    {
        //        var redisClient = this.Redis;
        //        EbTableCollection tcol = redisClient.Get<EbTableCollection>("EbInfraTableCollection");
        //        EbTableColumnCollection ccol = redisClient.Get<EbTableColumnCollection>("EbInfraTableColumnCollection");
        //        con.Open();
        //        var cmd = InfraDatabaseFactory.InfraDB.GetNewCommand(con, InfraDbSqlQueries["KEY1"]);
        //        foreach (string key in req.Parameters.Keys)
        //        {
        //            cmd.Parameters.Add(InfraDatabaseFactory.InfraDB.GetNewParameter(
        //                string.Format("@{0}", key), ccol[key].Type, req.Parameters[key]));

        //            foreach (int colkey in ccol.Keys)
        //            {
        //                if (ccol[colkey].Name == key)
        //                {
        //                }
        //            }
        //        }

        //        var dt = InfraDatabaseFactory.InfraDB.DoQuery(sql);
        //        ListDictionary list = new ListDictionary();
        //        foreach (EbDataRow dr in dt.Rows)
        //        {
        //            list.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
        //        }
        //        GetAccountResponse resp = new GetAccountResponse()
        //        {
        //            ldict = list
        //        };
        //        return resp;
        //    }
        //}
    }

    //internal class EmailServicesRequest1
    //{
    //    public string To { get; set; }
    //    public string Subject { get; set; }
    //    public string Message { get; set; }
    //    public string SolnId { get; set; }
    //}
}
