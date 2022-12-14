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
using ExpressBase.Security;
using ExpressBase.Common.LocationNSolution;
using ServiceStack.Auth;

namespace ExpressBase.ServiceStack.Services
{
    [ClientCanSwapTemplates]
    [EnableCors]
    [Authenticate]
    public class InfraServices : EbBaseService
    {
        public InfraServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }

        //public JoinbetaResponse Post(JoinbetaReq r)
        //{
        //    JoinbetaResponse resp = new JoinbetaResponse();
        //    try
        //    {
        //        string sql = string.Format("INSERT INTO eb_beta_enq(email,time) values('{0}','now()') RETURNING id", r.Email);
        //        var f = this.InfraConnectionFactory.DataDB.DoQuery(sql);
        //        if (f.Rows.Count > 0)
        //            resp.Status = true;
        //        else
        //            resp.Status = false;
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("Exception: " + e.ToString());
        //        resp.Status = false;
        //    }
        //    return resp;
        //}

        public GetVersioning Post(SolutionEditRequest request)
        {
            GetVersioning resp = new GetVersioning();
            try
            {
                string sql = string.Empty;
                if (request.ChangeColumn == solutionChangeColumn.version)
                    sql = string.Format("UPDATE eb_solutions SET versioning = true WHERE isolution_id = '{0}';", request.solution_id);
                else if (request.ChangeColumn == solutionChangeColumn.TwoFa)
                    sql = string.Format("UPDATE eb_solutions SET is2fa = {1}, otp_delivery_2fa = '{2}' WHERE isolution_id = '{0}';", request.solution_id, request.Value, request.DeliveryMethod);
                else if (request.ChangeColumn == solutionChangeColumn.OtpSignin)
                    sql = string.Format("UPDATE eb_solutions SET is_otp_signin = {1}, otp_delivery_signin = '{2}' WHERE isolution_id = '{0}';", request.solution_id, request.Value, request.DeliveryMethod);
                if (!string.IsNullOrEmpty(sql))
                {
                    int r = this.InfraConnectionFactory.DataDB.DoNonQuery(sql);
                    if (r > 0)
                    {
                        resp.res = true;
                    }
                }
            }
            catch (Exception e)
            {
                resp.res = false;
                Console.WriteLine("Exception: " + e.ToString());
                resp.status = new ResponseStatus { Message = e.Message };
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
            int _totalsolcount = 0;
            try
            {
                string sql = @"SELECT COUNT(*) FROM eb_solutions WHERE tenant_id = :tid AND pricing_tier = :pricing_tier AND type = 1;
                              SELECT COUNT(*) FROM eb_solutions WHERE tenant_id = :tid AND pricing_tier = :pricing_tier;";
                DbParameter[] parameters =
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("tid",EbDbTypes.Int32, request.UserId),
                this.InfraConnectionFactory.DataDB.GetNewParameter("pricing_tier",EbDbTypes.Int32, Convert.ToInt32(PricingTiers.FREE))
                };
                EbDataSet ds = this.InfraConnectionFactory.DataDB.DoQueries(sql, parameters);
                _solcount = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                _totalsolcount = Convert.ToInt32(ds.Tables[1].Rows[0][0]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error at count * of solutions :" + e.Message);
            }

            try
            {
                if (_solcount <= 3)
                {
                    CreateSolutionResponse response = this.Post(new CreateSolutionRequest
                    {
                        SolutionName = "My Solution " + (_totalsolcount + 1),
                        Description = "My solution " + (_totalsolcount + 1),
                        DeployDB = true,
                        UserId = request.UserId,
                        IsFurther = true,
                        PrimarySId = request.PrimarySId,
                        PackageId = request.PackageId
                    });
                    if (response.Id > 0)
                    {
                        resp.SolId = response.Id;
                        resp.Status = true;
                        User user = GetUserObject(request.UserAuthId);
                        user.Permissions.Add(response.SolURL + "-" + (int)SystemRoles.SolutionOwner);
                        this.Redis.Set<IUserAuth>(request.UserAuthId, user);
                    }
                }
                else
                    resp.Status = false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception at new solution creation furtherRequest :" + e.Message);
                resp.Status = false;
            }
            return resp;
        }

        public EditSolutionResponse Post(EditSolutionRequest request)
        {
            EditSolutionResponse resp = new EditSolutionResponse();

            string isid = this.Redis.Get<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, request.OldESolutionId));
            if (isid == null)
            {
                resp.Status = false;
                return resp;
            }
            string q;
            if (request.IsDelete)
                q = @"UPDATE 
                            eb_solutions 
                        SET 
                            esolution_id = :esid 
                        WHERE 
                            isolution_id = :isid 
                        AND 
                            tenant_id = :userid;";
            else
                q = @"UPDATE 
                            eb_solutions 
                        SET 
                            esolution_id = :esid,
                            solution_name = :sname,
                            description = :desc 
                        WHERE 
                            isolution_id = :isid 
                        AND 
                            tenant_id = :userid;";
            try
            {
                DbParameter[] parameters = new DbParameter[]
                {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("esid",EbDbTypes.String,request.NewESolutionId),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("sname",EbDbTypes.String,request.SolutionName),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("desc",EbDbTypes.String,request.Description),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("userid",EbDbTypes.Int32,request.UserId),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("isid",EbDbTypes.String,isid),
                };

                int rowaffcted = this.InfraConnectionFactory.DataDB.DoNonQuery(q, parameters);
                if (rowaffcted > 0)
                {
                    if (request.OldESolutionId == isid)
                        this.Redis.Set<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, request.NewESolutionId), isid);
                    else
                        this.Redis.RenameKey(string.Format(CoreConstants.SOLUTION_ID_MAP, request.OldESolutionId), string.Format(CoreConstants.SOLUTION_ID_MAP, request.NewESolutionId));
                    resp.Status = true;
                    TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
                    _tenantUserService.Post(new UpdateSolutionObjectRequest
                    {
                        SolnId = isid,
                        UserId = request.UserId
                    });
                }
                else
                    resp.Status = false;
            }
            catch (Exception e)
            {
                resp.Status = false;
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return resp;
        }

        public DeleteSolutionResponse Post(DeleteSolutionRequset request)
        {
            DeleteSolutionResponse res = new DeleteSolutionResponse();
            try
            {
                EditSolutionResponse resp = this.Post(new EditSolutionRequest
                {
                    OldESolutionId = request.ESolutionId,
                    NewESolutionId = request.ESolutionId + "_Deleted",
                    IsDelete = true,
                    Description = "",
                    SolutionName = "",
                    UserId = request.UserId
                });
                if (resp.Status)
                {
                    string q = "UPDATE eb_solutions SET eb_del = true WHERE isolution_id = :isolution_id";

                    DbParameter[] parameters = new DbParameter[]
                    {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("isolution_id",EbDbTypes.String,request.ISolutionId)
                    };
                    int st = this.InfraConnectionFactory.DataDB.DoNonQuery(q, parameters);
                    if (st > 0)
                        res.Status = true;
                    else
                        res.Status = false;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Message);
                res.Status = false;
            }
            return res;
        }

        public CleanupSolutionResponse Post(CleanupSolutionRequset request)
        {
            CleanupSolutionResponse res = new CleanupSolutionResponse();
            try
            {
                DevRelatedServices service = base.ResolveService<DevRelatedServices>();
                GetCleanupQueryResponse response = service.Post(new GetCleanupQueryRequest { SolnId = request.ISolutionId });
                Eb_Solution soln = GetSolutionObject(request.ISolutionId);
                if (response.CleanupQueries == string.Empty && soln.SolutionType == SolutionType.REPLICA)
                {
                    response = service.Post(new GetCleanupQueryRequest { SolnId = soln.PrimarySolution });
                }
                if (response.CleanupQueries != string.Empty)
                {
                    byte[] data = Convert.FromBase64String(response.CleanupQueries);
                    string query = Encoding.UTF8.GetString(data);
                    int st = (new EbConnectionFactory(request.ISolutionId, this.Redis)).DataDB.DoNonQuery(query);
                    res.Status = true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Message);
                res.Status = false;
            }
            return res;
        }

        public CheckSolutionOwnerResp Get(CheckSolutionOwnerReq request)
        {
            CheckSolutionOwnerResp resp = new CheckSolutionOwnerResp();
            try
            {
                string q = @"SELECT * FROM eb_solutions WHERE tenant_id = :userid AND esolution_id = :esid AND eb_del = false;";
                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("userid",EbDbTypes.Int32,request.UserId),
                this.InfraConnectionFactory.DataDB.GetNewParameter("esid",EbDbTypes.String,request.ESolutionId),
                };
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(q, parameters);

                if (dt.Rows.Count > 0)
                {
                    resp.IsValid = true;
                    resp.SolutionInfo.SolutionName = dt.Rows[0]["solution_name"].ToString();
                    resp.SolutionInfo.Description = dt.Rows[0]["description"].ToString();
                    resp.SolutionInfo.IsolutionId = dt.Rows[0]["isolution_id"].ToString();
                    resp.SolutionInfo.EsolutionId = dt.Rows[0]["esolution_id"].ToString();
                }
                else
                    resp.IsValid = false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                resp.IsValid = false;
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
                int sol_type = (!string.IsNullOrEmpty(request.PrimarySId) & request.PackageId > 0) ? 3 : 1;
                string sql = @" INSERT INTO eb_solutions (solution_name, tenant_id, date_created, description, solution_id, esolution_id, isolution_id, pricing_tier, type, primary_solution)
                                    VALUES(:sname, :tenant_id, now(), :descript, :solnid, :solnid, :solnid, 0 , :type, :primary) RETURNING id;	

                                INSERT INTO eb_role2tenant (tenant_id, solution_id, sys_role_id, eb_createdat, eb_createdby )
                                    VALUES (:tenant_id, :solnid, 0, NOW(), :tenant_id )RETURNING id;";

                DbParameter[] parameters = new DbParameter[]
                {
                    InfraConnectionFactory.DataDB.GetNewParameter("sname", EbDbTypes.String, request.SolutionName),
                    InfraConnectionFactory.DataDB.GetNewParameter("tenant_id", EbDbTypes.Int32, request.UserId),
                    InfraConnectionFactory.DataDB.GetNewParameter("descript", EbDbTypes.String, request.Description),
                    InfraConnectionFactory.DataDB.GetNewParameter("solnid", EbDbTypes.String, Sol_id_autogen),
                    InfraConnectionFactory.DataDB.GetNewParameter("type", EbDbTypes.Int32,sol_type),
                    InfraConnectionFactory.DataDB.GetNewParameter("primary", EbDbTypes.String, request.PrimarySId ?? string.Empty)
                };

                EbDataSet _ds = this.InfraConnectionFactory.DataDB.DoQueries(sql, parameters);
                resp.Id = Convert.ToInt32(_ds.Tables[0].Rows[0][0]);

                if (resp.Id > 0)
                {
                    //set esid=>isid map in redis
                    this.Redis.Set<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, Sol_id_autogen), Sol_id_autogen);
                    resp.SolURL = Sol_id_autogen;

                    if (request.DeployDB)
                    {
                        EbDbCreateResponse response = (EbDbCreateResponse)_dbService.Post(new EbDbCreateRequest
                        {
                            DBName = Sol_id_autogen,
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            IsChange = false,
                            IsFurther = request.IsFurther,
                            SolutionType = (SolutionType)sol_type
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

                            _tenantUserService.Post(new UpdateSolutionObjectRequest
                            {
                                SolnId = Sol_id_autogen,
                                UserId = request.UserId
                            });
                            if (!request.IsFurther || request.PackageId > 0)
                            {
                                ImportExportService service = base.ResolveService<ImportExportService>();
                                int demoAppId;
                                string env = Environment.GetEnvironmentVariable(EnvironmentConstants.ASPNETCORE_ENVIRONMENT);
                                Console.WriteLine("Environment : " + env);
                                if (env == "Staging" || env == "Development")
                                    demoAppId = 4;
                                else
                                    demoAppId = 13;
                                if (request.PackageId > 0)
                                    demoAppId = request.PackageId;
                                ImportApplicationResponse _response = service.Post(new ImportApplicationMqRequest
                                {
                                    Id = demoAppId,
                                    SolnId = Sol_id_autogen,
                                    UserId = request.UserId,
                                    UserAuthId = "",
                                    WhichConsole = "",
                                    IsDemoApp = true,
                                    SelectedSolutionId = Sol_id_autogen
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
            List<EbSolutionsWrapper> AllSolns = new List<EbSolutionsWrapper>();
            List<EbSolutionsWrapper> PrimarySolns = new List<EbSolutionsWrapper>();

            Dictionary<string, List<AppStore>> MasterApps = new Dictionary<string, List<AppStore>>();
            string sql = string.Format(@"SELECT * FROM eb_solutions WHERE tenant_id={0} AND eb_del=false; 
                                        SELECT * FROM eb_solutions WHERE tenant_id = {0} AND type = 2;
                                        SELECT id, app_name, user_solution_id FROM eb_appstore WHERE user_solution_id IN(SELECT isolution_id FROM eb_solutions WHERE tenant_id = {0} AND type = 2) AND is_master = true;", request.UserId);
            GetSolutionResponse resp = new GetSolutionResponse();
            try
            {
                EbDataSet ds = this.InfraConnectionFactory.DataDB.DoQueries(sql);
                foreach (EbDataRow dr in ds.Tables[0].Rows)
                {
                    EbSolutionsWrapper _ebSolutions = new EbSolutionsWrapper
                    {
                        SolutionName = dr[6].ToString(),
                        Description = dr[2].ToString(),
                        DateCreated = Convert.ToDateTime(dr[1]).ToString("g", DateTimeFormatInfo.InvariantInfo),
                        IsolutionId = dr[4].ToString(),
                        EsolutionId = dr[5].ToString(),
                        PricingTier = (PricingTiers)Convert.ToInt32(dr["pricing_tier"])
                    };
                    AllSolns.Add(_ebSolutions);
                }

                foreach (EbDataRow dr in ds.Tables[1].Rows)
                {
                    EbSolutionsWrapper _ebSolutions = new EbSolutionsWrapper
                    {
                        SolutionName = dr[6].ToString(),
                        Description = dr[2].ToString(),
                        DateCreated = Convert.ToDateTime(dr[1]).ToString("g", DateTimeFormatInfo.InvariantInfo),
                        IsolutionId = dr[4].ToString(),
                        EsolutionId = dr[5].ToString(),
                        PricingTier = (PricingTiers)Convert.ToInt32(dr["pricing_tier"])
                    };
                    PrimarySolns.Add(_ebSolutions);
                }

                foreach (EbDataRow dr in ds.Tables[2].Rows)
                {
                    if (!MasterApps.ContainsKey(dr[2].ToString()))
                    {
                        MasterApps.Add(dr[2].ToString(), new List<AppStore>());
                    }

                    AppStore _app = new AppStore
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString()
                    };
                    MasterApps[dr[2].ToString()].Add(_app);
                }

                resp.AllSolutions = AllSolns;
                resp.PrimarySolutions = PrimarySolns;
                resp.MasterPackages = MasterApps;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception:" + e.Message + e.StackTrace);
            }
            return resp;
        }

        public GetPrimarySolutionsResponse Get(GetPrimarySolutionsRequest request)
        {
            GetPrimarySolutionsResponse resp = new GetPrimarySolutionsResponse();
            List<EbSolutionsWrapper> PrimarySolns = new List<EbSolutionsWrapper>();
            string sql = string.Format(@"SELECT * FROM eb_solutions WHERE tenant_id = (SELECT tenant_id from eb_solutions where isolution_id = '{0}') AND type = 2;",
                request.SolnId);
            try
            {
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
                foreach (EbDataRow dr in dt.Rows)
                {
                    EbSolutionsWrapper _ebSolutions = new EbSolutionsWrapper
                    {
                        SolutionName = dr[6].ToString(),
                        Description = dr[2].ToString(),
                        DateCreated = Convert.ToDateTime(dr[1]).ToString("g", DateTimeFormatInfo.InvariantInfo),
                        IsolutionId = dr[4].ToString(),
                        EsolutionId = dr[5].ToString(),
                        PricingTier = (PricingTiers)Convert.ToInt32(dr["pricing_tier"])
                    };
                    PrimarySolns.Add(_ebSolutions);
                }
                resp.PrimarySolutions = PrimarySolns;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception:" + e.Message + e.StackTrace);
            }
            return resp;
        }

        public GetSolutioInfoResponse Get(GetSolutioInfoRequest request)
        {
            GetSolutioInfoResponse resp = null;
            try
            {
                Console.WriteLine("GetSolutioInfoRequest started - " + request.IsolutionId);
                ConnectionManager _conService = base.ResolveService<ConnectionManager>();
                string sql = string.Format(@"SELECT solution_name, description, date_created, esolution_id, pricing_tier, versioning, solution_settings, is2fa,
                                            otp_delivery_2fa, type, primary_solution, is_otp_signin, otp_delivery_signin, is_multilanguage FROM eb_solutions WHERE isolution_id='{0}' AND eb_del = false", request.IsolutionId);
                EbDataTable dt = (new EbConnectionFactory(CoreConstants.EXPRESSBASE, this.Redis)).DataDB.DoQuery(sql);
                if (dt.Rows.Count > 0)
                {
                    EbSolutionsWrapper _ebSolutions = new EbSolutionsWrapper
                    {
                        SolutionName = dt.Rows[0][0].ToString(),
                        Description = dt.Rows[0][1].ToString(),
                        DateCreated = dt.Rows[0][2].ToString(),
                        EsolutionId = dt.Rows[0][3].ToString(),
                        PricingTier = (PricingTiers)Convert.ToInt32(dt.Rows[0][4]),
                        IsVersioningEnabled = (bool)dt.Rows[0][5],
                        IsolutionId = request.IsolutionId,
                        SolutionSettings = JsonConvert.DeserializeObject<SolutionSettings>(dt.Rows[0][6].ToString()),
                        Is2faEnabled = (bool)dt.Rows[0][7],
                        OtpDelivery2fa = dt.Rows[0][8].ToString(),
                        SolutionType = (SolutionType)Convert.ToInt32(dt.Rows[0][9]),
                        PrimarySolution = dt.Rows[0][10].ToString(),
                        IsOtpSigninEnabled = (bool)dt.Rows[0][11],
                        OtpDeliverySignin = dt.Rows[0][12].ToString(),
                        IsMultiLanguageEnabled = (bool)dt.Rows[0][13],
                    };
                    resp = new GetSolutioInfoResponse() { Data = _ebSolutions };
                    if (resp.Data != null)
                    {
                        GetConnectionsResponse response = (GetConnectionsResponse)_conService.Post(new GetConnectionsRequest { ConnectionType = 0, SolutionId = request.IsolutionId });
                        resp.EBSolutionConnections = response.EBSolutionConnections;
                    }
                }
                else
                {
                    Console.WriteLine("Couldn't retrieve solution from db" + request.IsolutionId);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
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
											activation_code = null,
                                            mail_verify_time = NOW()
										WHERE 
											id = :id AND
											activation_code = :codes");

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
            bool unique;
            string pasword;
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
                    if (!string.IsNullOrEmpty(reqt.Fbid))
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
                string query;
                EbDataSet ds;
                if (reques.IsUser)
                {
                    query = String.Format(@"UPDATE eb_users 
										    SET resetpsw_code = :code
										    WHERE email = :mail AND eb_del='F';

                                            SELECT fullname 
                                            FROM eb_users 
										    WHERE  email=:mail AND eb_del='F';");
                    this.EbConnectionFactory = new EbConnectionFactory(reques.iSolutionId, Redis);
                    DbParameter[] parameters = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("code", EbDbTypes.String, reques.Resetcode),
                    this.EbConnectionFactory.DataDB.GetNewParameter("mail", EbDbTypes.String, reques.Email)
                    };
                    ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters);
                }
                else
                {
                    query = String.Format(@"UPDATE  eb_tenants 
										    SET resetpsw_code = :code
										    WHERE email = :mail AND eb_del='F';

                                            SELECT fullname 
                                            FROM eb_tenants 
										    WHERE  email=:mail AND eb_del='F';");
                    DbParameter[] parameters = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("code", EbDbTypes.String, reques.Resetcode),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("mail", EbDbTypes.String, reques.Email)
                    };
                    ds = this.InfraConnectionFactory.DataDB.DoQueries(query, parameters);
                }
                if (ds?.Tables[0]?.Rows?.Count > 0)
                {
                    string fullname = (ds.Tables[0].Rows[0][0]).ToString();
                    string aq = "$" + reques.Email + "$" + reques.Resetcode + "$";
                    byte[] plaintxt = System.Text.Encoding.UTF8.GetBytes(aq);
                    string ai = System.Convert.ToBase64String(plaintxt);
                    string resetlink = string.Format("https://{0}/resetpassword?rep={1}", reques.PageUrl, ai);

                    MessageProducer3.Publish(new EmailServicesRequest
                    {
                        To = reques.Email,
                        Subject = "Reset password",
                        Message = GenerateMailBody(resetlink, fullname),
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

        public string GenerateMailBody(string resetlink, string fullname)
        {
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
            body = body.Replace("{UserName}", fullname);
            body = body.Replace("{Url}", resetlink).Replace("{supporturl}", RoutingConstants.SUPPORT_MAIL_ID);

            return body;
        }

        public ResetPasswordResponse Post(ResetPasswordRequest reqst)
        {
            ResetPasswordResponse rests = new ResetPasswordResponse();
            try
            {
                string qur;
                int dt = 0;
                string hshpassword = (reqst.Password + reqst.Email).ToMD5Hash();
                if (reqst.IsUser)
                {
                    qur = String.Format(@"UPDATE 
										eb_users 
										SET
											pwd = :pswrd,
											resetpsw_code = null 
										WHERE 
											email = :id AND
											resetpsw_code = :codes");

                    this.EbConnectionFactory = new EbConnectionFactory(reqst.iSolutionId, this.Redis);
                    DbParameter[] parameters = { EbConnectionFactory.DataDB.GetNewParameter("pswrd",EbDbTypes.String,hshpassword),
                    EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.String, reqst.Email),
                    EbConnectionFactory.DataDB.GetNewParameter("codes", EbDbTypes.String, reqst.Resetcode) };
                    dt = this.EbConnectionFactory.DataDB.DoNonQuery(qur, parameters);
                }
                else
                {
                    qur = String.Format(@"UPDATE 
										eb_tenants 
										SET
											pwd = :pswrd,
											resetpsw_code = null 
										WHERE 
											email = :id AND
											resetpsw_code = :codes");

                    DbParameter[] parameters = { InfraConnectionFactory.DataDB.GetNewParameter("pswrd",EbDbTypes.String,hshpassword),
                    InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.String, reqst.Email),
                    InfraConnectionFactory.DataDB.GetNewParameter("codes", EbDbTypes.String, reqst.Resetcode) };
                    dt = this.InfraConnectionFactory.DataDB.DoNonQuery(qur, parameters);
                }

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

                using (var datacmd = dbf.DataDB.GetNewCommand(_con_d1, result))
                {
                    datacmd.ExecuteNonQuery();
                }
                using (var cmd = dbf.DataDB.GetNewCommand(_con_d1, "INSERT INTO eb_users(email,pwd,fullname,phnoprimary) VALUES(@email,@pwd,@fullname,@phnoprimary); INSERT INTO eb_role2user(user_id,role_id) VALUES(1,3)"))
                {
                    cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, dt.Rows[0][0]));
                    cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String, dt.Rows[0][3]));
                    cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("fullname", EbDbTypes.String, dt.Rows[0][1]));
                    cmd.Parameters.Add(EbConnectionFactory.DataDB.GetNewParameter("phnoprimary", EbDbTypes.String, dt.Rows[0][2]));
                    cmd.ExecuteScalar();
                }
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
                using (var datacmd = dbf.ObjectsDB.GetNewCommand(_con_o1, result))
                {
                    datacmd.ExecuteNonQuery();
                }
            }

        }


        public UpdateRedisConnectionsResponse Post(UpdateRedisConnectionsRequest request)
        {
            this.MessageProducer3.Publish(new UpdateRedisConnectionsMqRequest());
            return new UpdateRedisConnectionsResponse();
        }

        public UniqueRequestResponse Any(UniqueRequest request)
        {
            UniqueRequestResponse res = new UniqueRequestResponse();
            string sql;
            EbDataTable dt;
            if (!request.IsUser)
            {
                sql = "SELECT id, pwd FROM eb_tenants WHERE email = @email and eb_del='F'";
                DbParameter[] parameters = { this.InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, request.Email) };
                dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
            }
            else
            {
                sql = "SELECT id, pwd FROM eb_users WHERE email = @email and eb_del='F'";
                this.EbConnectionFactory = new EbConnectionFactory(request.iSolutionId, this.Redis);
                DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, request.Email) };
                dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
            }
            if (dt?.Rows?.Count > 0)
            {
                res.Unique = false;
                res.Id = Convert.ToInt32(dt.Rows[0]["id"]);
                res.HasPassword = !string.IsNullOrEmpty(dt.Rows[0]["pwd"].ToString());
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

        public UpdateSidMapResponse Post(UpdateSidMapRequest request)
        {
            string q = @"SELECT esolution_id, isolution_id FROM eb_solutions WHERE eb_del = false";
            string esid = string.Empty;
            string isid = string.Empty;
            if (!string.IsNullOrEmpty(request.ExtSolutionId))
            {
                q += " AND esolution_id = '" + request.ExtSolutionId + "';";
            }
            try
            {
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(q);
                if (dt?.Rows?.Count > 0)
                    foreach (EbDataRow row in dt.Rows)
                    {
                        esid = row["esolution_id"].ToString();
                        isid = row["isolution_id"].ToString();
                        if (string.IsNullOrEmpty(esid) || string.IsNullOrEmpty(isid))
                            continue;
                        else
                        {
                            this.Redis.Set<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, esid), isid);
                            this.Redis.Set<string>(string.Format(CoreConstants.SOLUTION_ID_MAP, isid), isid);
                        }
                    }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception at update sid map");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return new UpdateSidMapResponse();
        }

    }

}
