using System;
using System.Collections.Generic;
using System.Linq;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.Data;
using System.Data.Common;
using ExpressBase.Common;
using ExpressBase.Security.Core;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;
using System.Globalization;
using ServiceStack;
using Newtonsoft.Json;
using ExpressBase.Security;
using ServiceStack.Auth;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.ServiceClients;
using ServiceStack.Messaging;
using ExpressBase.Common.Constants;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class SecurityServices : EbBaseService
    {
        public SecurityServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IEbServerEventClient _sec) : base(_dbf, _mqp, _sec) { }

        //------COMMON LIST-------------------------------------

        public GetUsersResponse1 Any(GetUsersRequest1 request)
        {
            GetUsersResponse1 resp = new GetUsersResponse1();
            string show = string.Empty;
            if (request.Show != "all")
                show = "AND u.statusid >= 0 AND u.statusid <= 2 AND u.hide = 'no'";

            string sql = $@"
SELECT 
  u.id, u.fullname, u.email, u.nickname, u.sex, u.phnoprimary, u.statusid, ut.name, 
  COALESCE(CASE WHEN LENGTH(STRING_AGG(loc.shortname::TEXT, ', ')) > 12 THEN 
  SUBSTRING(STRING_AGG(loc.shortname::TEXT, ', '), 1, 12) || '...' ELSE STRING_AGG(loc.shortname::TEXT, ', ') END, 'Global') AS locname 
FROM 
  eb_users u 
LEFT JOIN eb_user_types ut ON u.eb_user_types_id = ut.id 
LEFT JOIN 
(   SELECT m.id, m.key_id, l.c_value
    FROM eb_constraints_master m, eb_constraints_line l
    WHERE m.id = l.master_id AND m.key_type = {(int)EbConstraintKeyTypes.User} AND 
    l.c_type = {(int)EbConstraintTypes.User_Location} AND eb_del = 'F' ORDER BY m.id
) cons ON u.id = cons.key_id
LEFT JOIN eb_locations loc ON loc.id=cons.c_value::INT 
WHERE COALESCE(u.eb_del, 'F') = 'F' AND COALESCE(ut.eb_del, 'F') = 'F' AND u.id > 1 {show}  GROUP BY u.id, ut.name ORDER BY u.fullname;";

            DbParameter[] parameters = { };

            var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

            List<Eb_User_ForCommonList> returndata = new List<Eb_User_ForCommonList>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                returndata.Add(new Eb_User_ForCommonList
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    Email = dr[2].ToString(),
                    Nick_Name = dr[3].ToString(),
                    Sex = dr[4].ToString(),
                    Phone_Number = dr[5].ToString(),
                    Status = ((EbUserStatus)Convert.ToInt32(dr[6])).ToString(),
                    User_Type = dr[7].ToString(),
                    Location = dr[8].ToString()
                });
            }
            resp.Data = returndata;

            return resp;
        } //for user search

        public GetAnonymousUserResponse Any(GetAnonymousUserRequest request)
        {
            GetAnonymousUserResponse resp = new GetAnonymousUserResponse();

            string sql = @"SELECT A.id, A.fullname, A.email, A.phoneno, A.socialid, A.firstvisit, A.lastvisit, A.totalvisits, B.applicationname 
								FROM eb_usersanonymous A 
								LEFT JOIN 
								eb_applications B
								ON  A.appid = B.id AND A.ebuserid = 1;";

            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

            var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

            List<Eb_AnonymousUser_ForCommonList> returndata = new List<Eb_AnonymousUser_ForCommonList>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                returndata.Add(new Eb_AnonymousUser_ForCommonList
                {
                    Id = Convert.ToInt32(dr[0]),
                    Full_Name = dr[1].ToString(),
                    Email_Id = dr[2].ToString(),
                    Phone_No = dr[3].ToString(),
                    Social_Id = dr[4].ToString(),
                    First_Visit = (dr[5].ToString() == DateTime.MinValue.ToString()) ? "" : dr[5].ToString(),
                    Last_Visit = (dr[6].ToString() == DateTime.MinValue.ToString()) ? "" : dr[6].ToString(),
                    Total_Visits = Convert.ToInt32(dr[7]),
                    App_Name = dr[8].ToString()
                });
            }
            resp.Data = returndata;

            return resp;
        }

        public GetUserGroupResponse1 Any(GetUserGroupRequest1 request)
        {
            GetUserGroupResponse1 resp = new GetUserGroupResponse1();

            EbDataSet dt;
            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

            if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL && request.Colvalues == null)
            {
                dt = this.EbConnectionFactory.DataDB.DoQueries((this.EbConnectionFactory.DataDB as MySqlDB).EB_GETUSERGROUP_QUERY_WITHOUT_SEARCHTEXT, parameters);
            }
            else
            {
                string sql = "SELECT id,name,description FROM eb_usergroup WHERE LOWER(name) LIKE LOWER('%' || :searchtext || '%') AND eb_del = 'F';";
                dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
            }

            List<Eb_UserGroup_ForCommonList> returndata = new List<Eb_UserGroup_ForCommonList>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                returndata.Add(new Eb_UserGroup_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString() });
            }
            resp.Data = returndata;

            return resp;
        } //for usergroup search

        public GetRolesResponse1 Any(GetRolesRequest1 request)
        {
            GetRolesResponse1 resp = new GetRolesResponse1();

            EbDataSet dt;
            //string sql = @"";
            //string sql = "SELECT R.id,R.role_name,R.description,A.applicationname FROM eb_roles R, eb_applications A WHERE R.applicationid = A.id AND R.role_name ~* @searchtext";

            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };
            if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL && (request.Colvalues == null))
            {
                dt = this.EbConnectionFactory.DataDB.DoQueries((this.EbConnectionFactory.DataDB as MySqlDB).EB_GETROLESRESPONSE_QUERY_WITHOUT_SEARCHTEXT, parameters);
            }
            else
            {
                dt = this.EbConnectionFactory.DataDB.DoQueries(this.EbConnectionFactory.DataDB.EB_GETROLESRESPONSE_QUERY, parameters);
            }
            List<Eb_Roles_ForCommonList> returndata = new List<Eb_Roles_ForCommonList>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                returndata.Add(new Eb_Roles_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString(), Application_Name = dr[3].ToString(), SubRole_Count = Convert.ToInt32(dr[4]), User_Count = Convert.ToInt32(dr[5]), Permission_Count = Convert.ToInt32(dr[6]) });
            }
            resp.Data = returndata;

            return resp;
        } //for roles search



        //----MANAGE USER START---------------------------------
        public GetManageUserResponse Any(GetManageUserRequest request)
        {
            GetManageUserResponse resp = new GetManageUserResponse();
            if (request.RqstMode == 3)//Mode == 3 for MyProfile View
            {
                request.Id = request.UserId;
            }
            string sql = this.EbConnectionFactory.DataDB.EB_MANAGEUSER_FIRST_QUERY;
            if (request.Id > 1)
            {
                sql += @"SELECT fullname, nickname, email, alternateemail, dob, sex, phnoprimary, phnosecondary, landline, phextension, fbid, fbname, statusid, hide, preferencesjson, dprefid, eb_user_types_id, forcepwreset
						FROM eb_users WHERE id = @id AND (statusid = 0 OR statusid = 1 OR statusid = 2) AND id > 1 AND eb_del = 'F';
						SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = 'F';
						SELECT groupid FROM eb_user2usergroup WHERE userid = @id AND eb_del = 'F';";

                sql += EbConstraints.GetSelectQuery(EbConstraintKeyTypes.User, EbConnectionFactory.DataDB);

                //SELECT m.id, m.key_id, m.key_type, m.description, l.id AS lid, l.c_type, l.c_operation, l.c_value 
                //            FROM eb_constraints_master m, eb_constraints_line l
                //            WHERE m.id = l.master_id AND m.key_id = :id AND key_type = 1 AND eb_del = 'F' ORDER BY m.id;
                //SELECT id, user_id, usergroup_id, role_id, c_type, c_value, c_operation, c_meta FROM eb_constraints WHERE user_id = :id AND eb_del = 'F' ORDER BY id;
            }
            //SELECT firstname, email, socialid, socialname FROM eb_users WHERE id = @id;	old 4th query
            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };
            var ds = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

            resp.Roles = new List<EbRole>();
            foreach (var dr in ds.Tables[0].Rows)
            {
                resp.Roles.Add(new EbRole
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    Description = dr[2].ToString()
                });
            }

            resp.EbUserGroups = new List<EbUserGroups>();
            foreach (var dr in ds.Tables[1].Rows)
            {
                resp.EbUserGroups.Add(new EbUserGroups
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    Description = dr[2].ToString()

                });
            }

            resp.Role2RoleList = new List<Eb_RoleToRole>();
            foreach (EbDataRow dr in ds.Tables[2].Rows)
            {
                resp.Role2RoleList.Add(new Eb_RoleToRole()
                {
                    Id = Convert.ToInt32(dr[0]),
                    Dominant = Convert.ToInt32(dr[1]),
                    Dependent = Convert.ToInt32(dr[2])
                });
            }

            resp.UserTypes = new Dictionary<int, string>();
            foreach (EbDataRow dr in ds.Tables[3].Rows)
            {
                resp.UserTypes.Add(Convert.ToInt32(dr[0]), Convert.ToString(dr[1]));
            }

            if (request.Id > 1)
            {
                resp.UserData = new Dictionary<string, string>();
                if (ds.Tables[4].Rows.Count == 0)
                    return resp;
                foreach (var dr in ds.Tables[4].Rows)
                {
                    resp.UserData.Add("id", request.Id.ToString());
                    resp.UserData.Add("fullname", dr[0].ToString());
                    resp.UserData.Add("nickname", dr[1].ToString());
                    resp.UserData.Add("email", dr[2].ToString());
                    resp.UserData.Add("alternateemail", dr[3].ToString());
                    resp.UserData.Add("dob", Convert.ToDateTime(dr[4]).ToString("yyyy-MM-dd"));
                    resp.UserData.Add("sex", dr[5].ToString());
                    resp.UserData.Add("phnoprimary", dr[6].ToString());
                    resp.UserData.Add("phnosecondary", dr[7].ToString());
                    resp.UserData.Add("landline", dr[8].ToString());
                    resp.UserData.Add("phextension", dr[9].ToString());
                    resp.UserData.Add("fbid", dr[10].ToString());
                    resp.UserData.Add("fbname", dr[11].ToString());
                    resp.UserData.Add("statusid", dr[12].ToString());
                    resp.UserData.Add("hide", dr[13].ToString());
                    resp.UserData.Add("preference", dr[14].ToString());
                    resp.UserData.Add("dprefid", dr[15].ToString());
                    resp.UserData.Add("eb_user_types_id", dr[16].ToString());
                    resp.UserData.Add("forcepwreset", dr[17].ToString());
                }

                resp.UserRoles = new List<int>();
                foreach (var dr in ds.Tables[5].Rows)
                    resp.UserRoles.Add(Convert.ToInt32(dr[0]));

                resp.UserGroups = new List<int>();
                foreach (var dr in ds.Tables[6].Rows)
                    resp.UserGroups.Add(Convert.ToInt32(dr[0]));

                EbConstraints con = new EbConstraints(ds.Tables[7]);
                resp.LocConstraint = new Dictionary<int, int>();
                foreach (var c in con.UConstraints)
                {
                    if (c.Value.Values.ElementAt(0).Value.Type == EbConstraintTypes.User_Location)
                        resp.LocConstraint.Add(c.Key, c.Value.Values.ElementAt(0).Value.GetValue());
                }
            }

            return resp;
        }

        //public UniqueCheckResponse Any(UniqueCheckRequest request)
        //{
        //    string sql = string.Empty;
        //    DbParameter[] parameters = new DbParameter[] { };

        //    if (!string.IsNullOrEmpty(request.email))
        //    {
        //        sql = "SELECT id FROM eb_users WHERE LOWER(email) LIKE LOWER(@email) AND eb_del = 'F' AND (statusid = 0 OR statusid = 1 OR statusid = 2);";
        //        parameters = new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, string.IsNullOrEmpty(request.email) ? "" : request.email) };
        //    }
        //    else if (!string.IsNullOrEmpty(request.roleName))
        //    {
        //        sql = "SELECT id FROM eb_roles WHERE LOWER(role_name) LIKE LOWER(@roleName)";
        //        parameters = new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("roleName", EbDbTypes.String, string.IsNullOrEmpty(request.roleName) ? "" : request.roleName) };
        //    }
        //    var dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
        //    if (dt.Rows.Count > 0)
        //    {
        //        return new UniqueCheckResponse { unrespose = true };
        //    }
        //    else
        //    {
        //        return new UniqueCheckResponse { unrespose = false };
        //    }
        //}

        public UniqueCheckResponse Any(UniqueCheckRequest request)
        {
            string Qry = string.Empty;
            DbParameter[] Params = new DbParameter[]
            {
                this.EbConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String, request.Value),
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
            };

            if (request.QryId == UniqueCheckQueryId.eb_users__email)
                Qry = "SELECT id FROM eb_users WHERE LOWER(email) LIKE LOWER(@val) AND COALESCE(eb_del, 'F') = 'F' AND id <> @id;";
            else if (request.QryId == UniqueCheckQueryId.eb_users__phnoprimary)
                Qry = "SELECT id FROM eb_users WHERE LOWER(phnoprimary) LIKE LOWER(@val) AND COALESCE(eb_del, 'F') = 'F' AND id <> @id;";
            else if (request.QryId == UniqueCheckQueryId.eb_roles__role_name)
                Qry = "SELECT id FROM eb_roles WHERE LOWER(role_name) LIKE LOWER(@val) AND COALESCE(eb_del, 'F') = 'F' AND id <> @id";
            else if (request.QryId == UniqueCheckQueryId.eb_users__nickname)
                Qry = "SELECT id FROM eb_users WHERE LOWER(nickname) LIKE LOWER(@val) AND COALESCE(eb_del, 'F') = 'F' AND id <> @id";

            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry, Params);
            if (dt.Rows.Count == 0)
                return new UniqueCheckResponse { unrespose = true };
            else
                return new UniqueCheckResponse { unrespose = false };
        }

        public ResetUserPasswordResponse Any(ResetUserPasswordRequest request)
        {
            string sql = "UPDATE eb_users SET pwd = @newpwd, pw = @newpw WHERE id = @userid;";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.Id),
                this.EbConnectionFactory.DataDB.GetNewParameter("newpwd", EbDbTypes.String, (request.NewPwd + request.Email).ToMD5Hash()),
                this.EbConnectionFactory.DataDB.GetNewParameter("newpw", EbDbTypes.String, (request.NewPwd.ToMD5Hash() + request.UserId.ToString() + request.SolnId).ToMD5Hash()),
            };
            return new ResetUserPasswordResponse()
            {
                isSuccess = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters) > 0 ? true : false
            };
        }

        public SaveUserResponse Post(SaveUserRequest request)
        {
            SaveUserResponse resp;
            string sql = this.EbConnectionFactory.DataDB.EB_SAVEUSER_QUERY;
            int id = 0;

            if (!request.LocationAdd.IsNullOrEmpty())
            {
                EbConstraints consObj = new EbConstraints(request.LocationAdd.Split(","), EbConstraintKeyTypes.User, EbConstraintTypes.User_Location);
                request.LocationAdd = consObj.GetDataAsString();
            }

            List<string> OldRole_Ids = new List<string>();
            try
            {
                EbDataTable edu = this.EbConnectionFactory.DataDB.DoQuery($"SELECT role_id FROM  eb_role2user WHERE COALESCE(eb_del, 'F') = 'F' AND user_id = :id;",
                            new DbParameter[]{
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                            });
                if (edu.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in edu.Rows)
                    {
                        OldRole_Ids.Add(dr[0].ToString());
                    }
                }

            }
            catch (Exception e1)
            {
                Console.WriteLine("Error while fetch role_ids from eb_role2user " + e1.Message + e1.StackTrace);
            }

            List<string> OldUsrGrp_Ids = new List<string>();
            try
            {
                EbDataTable edt = this.EbConnectionFactory.DataDB.DoQuery(@"SELECT groupid FROM eb_user2usergroup WHERE userid = @id AND eb_del = 'F';",
                            new DbParameter[]{
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                            });
                if (edt.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in edt.Rows)
                    {
                        OldUsrGrp_Ids.Add(dr[0].ToString());
                    }
                }

            }
            catch (Exception e1)
            {
                Console.WriteLine("Error while fetch user groupid from eb_user2usergroup " + e1.Message + e1.StackTrace);
            }


            //string password = (request.Password + request.EmailPrimary).ToMD5Hash();
            List<DbParameter> parameters = new List<DbParameter> {
                this.EbConnectionFactory.DataDB.GetNewParameter("_userid", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("_id", EbDbTypes.Int32, request.Id),
                this.EbConnectionFactory.DataDB.GetNewParameter("_fullname", EbDbTypes.String, request.FullName),
                this.EbConnectionFactory.DataDB.GetNewParameter("_nickname", EbDbTypes.String, request.NickName),
                this.EbConnectionFactory.DataDB.GetNewParameter("_email", EbDbTypes.String, request.EmailPrimary),
                this.EbConnectionFactory.DataDB.GetNewParameter("_pwd", EbDbTypes.String, request.Password),
                this.EbConnectionFactory.DataDB.GetNewParameter("_dob", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(request.DateOfBirth, "yyyy-MM-dd", CultureInfo.InvariantCulture))),
                this.EbConnectionFactory.DataDB.GetNewParameter("_sex", EbDbTypes.String, request.Sex),
                this.EbConnectionFactory.DataDB.GetNewParameter("_alternateemail", EbDbTypes.String, request.EmailSecondary),
                this.EbConnectionFactory.DataDB.GetNewParameter("_phprimary", EbDbTypes.String, request.PhonePrimary),
                this.EbConnectionFactory.DataDB.GetNewParameter("_phsecondary", EbDbTypes.String, request.PhoneSecondary),
                this.EbConnectionFactory.DataDB.GetNewParameter("_phlandphone", EbDbTypes.String, request.LandPhone),
                this.EbConnectionFactory.DataDB.GetNewParameter("_extension", EbDbTypes.String, request.PhoneExtension),
                this.EbConnectionFactory.DataDB.GetNewParameter("_fbid", EbDbTypes.String, request.FbId),
                this.EbConnectionFactory.DataDB.GetNewParameter("_fbname", EbDbTypes.String, request.FbName),
                this.EbConnectionFactory.DataDB.GetNewParameter("_roles", EbDbTypes.String, (request.Roles != string.Empty? request.Roles : string.Empty)),
                this.EbConnectionFactory.DataDB.GetNewParameter("_groups", EbDbTypes.String, (request.UserGroups != string.Empty? request.UserGroups : string.Empty)),
                this.EbConnectionFactory.DataDB.GetNewParameter("_statusid", EbDbTypes.Int32, Convert.ToInt32(request.StatusId)),
                this.EbConnectionFactory.DataDB.GetNewParameter("_hide", EbDbTypes.String, request.Hide),
                this.EbConnectionFactory.DataDB.GetNewParameter("_anonymoususerid", EbDbTypes.Int32, request.AnonymousUserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("_preferences", EbDbTypes.String, request.Preference),
                this.EbConnectionFactory.DataDB.GetNewParameter("_usertype", EbDbTypes.Int32, request.UserType),
                this.EbConnectionFactory.DataDB.GetNewParameter("_consadd", EbDbTypes.String, request.LocationAdd),
                this.EbConnectionFactory.DataDB.GetNewParameter("_consdel", EbDbTypes.String, request.LocationDelete),
                this.EbConnectionFactory.DataDB.GetNewParameter("_forcepwreset", EbDbTypes.String, request.ForceResetPassword),
                this.EbConnectionFactory.DataDB.GetNewParameter("_isolution_id", EbDbTypes.String, request.SolnId)
            };

            if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
            {
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewOutParameter("out_uid", EbDbTypes.Int32));
                EbDataTable ds = EbConnectionFactory.DataDB.DoProcedure(EbConnectionFactory.DataDB.EB_SAVEUSER_QUERY, parameters.ToArray());

                if (ds.Rows.Count > 0)
                {
                    id = Int32.Parse(ds.Rows[0][0].ToString());
                }
            }
            else
            {
                EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters.ToArray());
                id = Convert.ToInt32(dt.Tables[0].Rows[0][0]);
            }

            TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
            _tenantUserService.Post(new UpdateSolutionObjectRequest() { SolnId = request.SolnId, UserId = request.UserId });

            resp = new SaveUserResponse
            {
                id = Convert.ToInt32(id)
            };
            //if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
            //{
            //	using (var service = base.ResolveService<EmailService>())
            //	{
            //		//  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
            //	}
            //}

            if (request.Id == 0 && id > 0)
            {
                try
                {
                    var service = base.ResolveService<NotificationService>();
                    var r = (NotifyByUserRoleResponse)service.Post(new NotifyByUserRoleRequest
                    {
                        Link = $"/Security/ManageUser?itemid={id}&Mode=2",
                        Title = "New User " + request.FullName + " Created",
                        RoleID = new List<int>
                            {
                                (int)SystemRoles.SolutionOwner,
                                (int)SystemRoles.SolutionAdmin,
                                (int)SystemRoles.SolutionPM
                            },
                        SolutionId = request.SolnId
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message + e.StackTrace);
                }
            }
            ////server events
            else
            {
                try
                {
                    this.MessageProducer3.Publish(new SaveUserMqRequest
                    {
                        BToken = this.ServerEventClient.BearerToken,
                        RToken = this.ServerEventClient.RefreshToken,
                        SolnId = request.SolnId,
                        UserId = request.Id,
                        LocationDelete = request.LocationDelete,
                        LocationAdd = request.LocationAdd,
                        NewRole_Ids = request.Roles.Split(",").ToList(),
                        OldRole_Ids = OldRole_Ids,
                        NewUserGroups = request.UserGroups.Split(",").ToList(),
                        OldUserGroups = OldUsrGrp_Ids,
                        WhichConsole = "uc"
                    });

                }
                catch (Exception ex)
                {
                    Console.WriteLine("save user - message queue" + ex.Message + ex.StackTrace);
                }
            }
            if (Convert.ToInt32(request.StatusId) == 1 || Convert.ToInt32(request.StatusId) == 2 || Convert.ToInt32(request.StatusId) == 3)
            {
                try
                {
                    string authid = request.SolnId + CharConstants.COLON + request.Id + CharConstants.COLON + RoutingConstants.MC;
                    IUserAuth usr = this.Redis.Get<IUserAuth>(authid);
                    if (usr != null)
                        this.Redis.Remove(authid);
                    //code here to refresh mob app via push msg
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception while tried to remove mc context: " + ex.Message + ex.StackTrace);
                }

                try
                {
                    this.MessageProducer3.Publish(new SuspendUserMqRequest
                    {
                        BToken = this.ServerEventClient.BearerToken,
                        RToken = this.ServerEventClient.RefreshToken,
                        SolnId = request.SolnId,
                        UserId = request.Id
                    });

                }
                catch (Exception ex)
                {
                    Console.WriteLine("suspand user - message queue" + ex.Message + ex.StackTrace);
                }
            }
            return resp;
        }

        public DeleteUserResponse Post(DeleteUserRequest request)
        {
            string sql = @"INSERT INTO eb_userstatus(userid, statusid, createdby, createdat) VALUES (@id, 3, @userid, NOW());
                            UPDATE eb_users SET statusid = 3 WHERE id = @id AND eb_del = 'F';";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId)
            };
            int t = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters.ToArray());
            if (t > 0)
            {
                TenantUserServices _tenantUserService = base.ResolveService<TenantUserServices>();
                _tenantUserService.Post(new UpdateSolutionObjectRequest() { SolnId = request.SolnId, UserId = request.UserId });
            }

            return new DeleteUserResponse() { Status = t };
        }

        //------MY PROFILE------------------------------------------------------

        public GetMyProfileResponse Any(GetMyProfileRequest request)
        {
            Dictionary<string, string> userData = new Dictionary<string, string>();
            Dictionary<string, string> RefIds = new Dictionary<string, string>();
            EbDataSet ds;
            if (request.WC == RoutingConstants.TC)
            {
                string selQry = @"SELECT fullname,email,alternate_email,dob,sex,ph_primary,ph_secondary,ph_landline,ph_land_extensn,preferences_json
						FROM eb_tenants WHERE id = :id";
                DbParameter[] parameters = { this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.UserId) };
                ds = this.InfraConnectionFactory.DataDB.DoQueries(selQry, parameters);
            }
            else
            {
                string selQry = @"SELECT fullname,nickname,email,alternateemail,dob,sex,phnoprimary,phnosecondary,landline,phextension,preferencesjson
						FROM eb_users WHERE id = @id ; 
                            SELECT t2.* FROM
                                (
	                                SELECT 
 		                                q.ver_id as ver_id FROM( 
			                                SELECT 
				                                eos.eb_obj_ver_id as ver_id, eos.status as t_status 
			                                FROM 
    			                                eb_objects_status eos WHERE eos.id IN (
					                                SELECT MAX(eos1.id) AS id1 FROM eb_objects_status eos1 WHERE eos1.eb_obj_ver_id IN(
						                                SELECT eov.id FROM eb_objects_ver eov, eb_objects eo 
                                                        WHERE  eov.eb_objects_id = eo.id And eo.obj_type = 22
                                                        {0}
                                                        AND eov.eb_objects_id = eo.id 
                                                        AND coalesce(eov.eb_del,'F')='F' 
                                                        AND coalesce(eo.eb_del,'F')='F' ) 
                                                        GROUP BY eos1.eb_obj_ver_id )
				                                )q WHERE t_status=3
                                ) t1
                                LEFT JOIN				
                                (
                                SELECT 
 	                                eov.eb_objects_id, eov.id AS ver_id, eov.refid,eo.display_name
                                FROM
	                                eb_objects_ver eov,eb_objects eo
						        WHERE	eo.id = eov.eb_objects_id
                                )t2
                                ON t1.ver_id = t2.ver_id;";

                if (request.IsSolutionOwner)
                {
                    selQry = string.Format(selQry, string.Empty);
                    DbParameter[] para = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.UserId),
                    };
                    ds = this.EbConnectionFactory.DataDB.DoQueries(selQry, para);
                }
                else
                {
                    selQry = string.Format(selQry, EbConnectionFactory.DataDB.EB_GET_MYPROFILE_OBJID);
                    DbParameter[] param2 =
                    {
                        this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.UserId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",",request.DBIds))
                    };
                    ds = this.EbConnectionFactory.DataDB.DoQueries(selQry, param2);
                }


                //parameters = { 
                //    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.UserId),
                //    this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",",request.DBIds))
                //};
                //ds = this.EbConnectionFactory.DataDB.DoQueries(selQry, parameters);
            }
            if (ds.Tables[0].Rows.Count > 0)
            {
                EbDataTable dt = ds.Tables[0];
                userData.Add("fullname", dt.Rows[0]["fullname"].ToString());
                userData.Add("email", dt.Rows[0]["email"].ToString());
                userData.Add("dob", Convert.ToDateTime(dt.Rows[0]["dob"]).ToString("dd-MM-yyyy"));
                userData.Add("sex", dt.Rows[0]["sex"].ToString());
                if (request.WC == RoutingConstants.TC)
                {
                    userData.Add("alternate_email", dt.Rows[0]["alternate_email"].ToString());
                    userData.Add("ph_primary", dt.Rows[0]["ph_primary"].ToString());
                    userData.Add("ph_secondary", dt.Rows[0]["ph_secondary"].ToString());
                    userData.Add("ph_landline", dt.Rows[0]["ph_landline"].ToString());
                    userData.Add("ph_land_extensn", dt.Rows[0]["ph_land_extensn"].ToString());
                    userData.Add("preferences_json", dt.Rows[0]["preferences_json"].ToString());
                }
                else
                {
                    userData.Add("nickname", dt.Rows[0]["nickname"].ToString());
                    userData.Add("alternateemail", dt.Rows[0]["alternateemail"].ToString());
                    userData.Add("phnoprimary", dt.Rows[0]["phnoprimary"].ToString());
                    userData.Add("phnosecondary", dt.Rows[0]["phnosecondary"].ToString());
                    userData.Add("landline", dt.Rows[0]["landline"].ToString());
                    userData.Add("phextension", dt.Rows[0]["phextension"].ToString());
                    userData.Add("preferencesjson", dt.Rows[0]["preferencesjson"].ToString());

                    if (ds.Tables[1].Rows.Count > 0)
                    {
                        for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                        {
                            RefIds.Add(ds.Tables[1].Rows[i]["refid"].ToString(), ds.Tables[1].Rows[i]["display_name"].ToString());
                        }
                    }
                }
            }

            return new GetMyProfileResponse { UserData = userData, RefIds = RefIds };
        }

        public SaveMyProfileResponse Any(SaveMyProfileRequest request)
        {
            List<KeyValueType_Field> Fields = JsonConvert.DeserializeObject<List<KeyValueType_Field>>(request.UserData);
            var dict = Fields.ToDictionary(x => x.Key);
            KeyValueType_Field found;
            List<DbParameter> parameters = new List<DbParameter>();
            string upcolsvals = string.Empty;
            EbConnectionFactory ConnectionFactory = null;
            if (request.WC == RoutingConstants.TC)
                ConnectionFactory = this.InfraConnectionFactory;
            else
                ConnectionFactory = this.EbConnectionFactory;
            if (!request.PreferenceOnly)
            {
                if (dict.TryGetValue("fullname", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "fullname=@fullname,";
                }
                if (dict.TryGetValue("nickname", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "nickname=@nickname,";
                }
                if (dict.TryGetValue("alternateemail", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "alternateemail=@alternateemail,";
                }
                if (dict.TryGetValue("dob", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture))));
                    upcolsvals += "dob=@dob,";
                }
                if (dict.TryGetValue("sex", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "sex=@sex,";
                }
                if (dict.TryGetValue("phnoprimary", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "phnoprimary=@phnoprimary,";
                }
                if (dict.TryGetValue("phnosecondary", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "phnosecondary=@phnosecondary,";
                }
                if (dict.TryGetValue("landline", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "landline=@landline,";
                }
                if (dict.TryGetValue("phextension", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "phextension=@phextension,";
                }
                //----------------------------
                if (dict.TryGetValue("alternate_email", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "alternate_email=@alternate_email,";
                }
                if (dict.TryGetValue("ph_primary", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "ph_primary=@ph_primary,";
                }
                if (dict.TryGetValue("ph_secondary", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "ph_secondary=@ph_secondary,";
                }
                if (dict.TryGetValue("ph_landline", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "ph_landline=@ph_landline,";
                }
                if (dict.TryGetValue("ph_land_extensn", out found))
                {
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value.ToString()));
                    upcolsvals += "ph_land_extension=@ph_land_extension,";
                }
            }

            if (dict.TryGetValue("preferencesjson", out found))
            {
                try
                {
                    var temp = JsonConvert.DeserializeObject<Preferences>(found.Value.ToString());
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, JsonConvert.SerializeObject(temp)));
                    upcolsvals += "preferencesjson=@preferencesjson,";
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed - preferencesjson may not be in correct format  : " + ex.Message);
                }
            }
            if (dict.TryGetValue("preferences_json", out found))
            {
                try
                {
                    var temp = JsonConvert.DeserializeObject<Preferences>(found.Value.ToString());
                    parameters.Add(ConnectionFactory.DataDB.GetNewParameter(found.Key, EbDbTypes.String, JsonConvert.SerializeObject(temp)));
                    upcolsvals += "preferences_json=@preferences_json,";
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed - preferences_json may not be in correct format  : " + ex.Message);
                }
            }
            parameters.Add(ConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.UserId));
            var rstatus = 0;
            if (request.WC == RoutingConstants.TC)
            {
                string Qry = string.Format("UPDATE {0} SET {1} WHERE id=:id", "eb_tenants", upcolsvals.Substring(0, upcolsvals.Length - 1));
                rstatus = this.InfraConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());
            }
            else
            {
                string Qry = string.Format("UPDATE {0} SET {1} WHERE id=@id", "eb_users", upcolsvals.Substring(0, upcolsvals.Length - 1));
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(Qry, parameters.ToArray());
            }
            return new SaveMyProfileResponse { RowsAffectd = rstatus };
        }

        public ChangeUserPasswordResponse Any(ChangeUserPasswordRequest request)
        {
            string sql = "UPDATE {0} SET pwd = :newpwd WHERE id = :userid AND pwd = :oldpwd;";
            int stus = 0;
            if (request.WC == RoutingConstants.TC)
            {
                DbParameter[] parameters = new DbParameter[] {
                this.InfraConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                this.InfraConnectionFactory.DataDB.GetNewParameter("oldpwd", EbDbTypes.String, (request.OldPwd + request.Email).ToMD5Hash()),
                this.InfraConnectionFactory.DataDB.GetNewParameter("newpwd", EbDbTypes.String, (request.NewPwd + request.Email).ToMD5Hash())
                };
                stus = this.InfraConnectionFactory.DataDB.DoNonQuery(string.Format(sql, "eb_tenants"), parameters);
            }
            else
            {
                DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("oldpwd", EbDbTypes.String, (request.OldPwd + request.Email).ToMD5Hash()),
                this.EbConnectionFactory.DataDB.GetNewParameter("newpwd", EbDbTypes.String, (request.NewPwd + request.Email).ToMD5Hash())
                };
                stus = this.EbConnectionFactory.DataDB.DoNonQuery(string.Format(sql, "eb_users"), parameters);
            }
            return new ChangeUserPasswordResponse()
            {
                isSuccess = stus > 0 ? true : false
            };
        }

        //------MANAGE ANONYMOUS USER START----------------------------

        public GetManageAnonymousUserResponse Any(GetManageAnonymousUserRequest request)
        {
            Dictionary<string, string> Udata = new Dictionary<string, string>();
            string sql = @"SELECT A.id, A.fullname, A.email, A.phoneno, A.socialid, A.firstvisit, A.lastvisit, A.totalvisits, B.applicationname, A.remarks, A.browser, A.ipaddress
								FROM eb_usersanonymous A, eb_applications B
								WHERE A.appid = B.id AND A.ebuserid = 1 AND A.id = @id;
							SELECT B.fullname, A.modifiedat FROM eb_usersanonymous A, eb_users B 
								WHERE A.modifiedby = B.id AND A.id = @id;";

            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };
            var ds = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
            if (ds.Tables.Count > 1)
            {
                Udata.Add("FullName", ds.Tables[0].Rows[0][1].ToString());
                Udata.Add("Email", ds.Tables[0].Rows[0][2].ToString());
                Udata.Add("Phone", ds.Tables[0].Rows[0][3].ToString());
                Udata.Add("SocialId", ds.Tables[0].Rows[0][4].ToString());
                Udata.Add("FirstVisit", ds.Tables[0].Rows[0][5].ToString());
                Udata.Add("LastVisit", ds.Tables[0].Rows[0][6].ToString());
                Udata.Add("TotalVisits", ds.Tables[0].Rows[0][7].ToString());
                Udata.Add("ApplicationName", ds.Tables[0].Rows[0][8].ToString());
                Udata.Add("Remarks", ds.Tables[0].Rows[0][9].ToString());
                Udata.Add("Browser", ds.Tables[0].Rows[0][10].ToString());
                Udata.Add("IpAddress", ds.Tables[0].Rows[0][11].ToString());
                if (ds.Tables[1].Rows.Count > 0)
                {
                    Udata.Add("ModifiedBy", ds.Tables[1].Rows[0][0].ToString());
                    Udata.Add("ModifiedAt", ds.Tables[1].Rows[0][1].ToString());
                }
                else
                {
                    Udata.Add("ModifiedBy", "");
                    Udata.Add("ModifiedAt", "");
                }
            }
            return new GetManageAnonymousUserResponse { UserData = Udata };
        }

        public UpdateAnonymousUserResponse Any(UpdateAnonymousUserRequest request)
        {
            string sql = @"UPDATE eb_usersanonymous 
								SET fullname=@fullname, email=@emailid, phoneno=@phoneno, remarks = @remarks, modifiedby = @modifiedby, modifiedat = @NOW
								WHERE id=@id";
            DbParameter[] parameters = {
                this.EbConnectionFactory.DataDB.GetNewParameter("fullname", EbDbTypes.String, request.FullName),
                this.EbConnectionFactory.DataDB.GetNewParameter("emailid", EbDbTypes.String, request.EmailID),
                this.EbConnectionFactory.DataDB.GetNewParameter("phoneno", EbDbTypes.String, request.PhoneNumber),
                this.EbConnectionFactory.DataDB.GetNewParameter("remarks", EbDbTypes.String, request.Remarks),
                this.EbConnectionFactory.DataDB.GetNewParameter("modifiedby", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("NOW", EbDbTypes.DateTime, DateTime.Now),
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
            };
            return new UpdateAnonymousUserResponse { RowAffected = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters) };
        }

        //------MANAGE USER GROUP START------------------------------

        public GetManageUserGroupResponse Any(GetManageUserGroupRequest request)
        {
            List<Eb_Users> _usersList = new List<Eb_Users>();
            List<Eb_Constraints1> _ipConsList = new List<Eb_Constraints1>();
            List<Eb_Constraints1> _dtConsList = new List<Eb_Constraints1>();
            Dictionary<string, object> _userGroupInfo = new Dictionary<string, object>();
            List<Eb_Users> _usersListAll = new List<Eb_Users>();

            string Qry = @"SELECT id, fullname, email, phnoprimary FROM eb_users WHERE COALESCE(eb_del, 'F') = 'F' ORDER BY fullname, email, phnoprimary;";
            if (request.id > 0)
            {
                Qry += @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
						SELECT U.id,U.fullname,U.email FROM eb_users U, eb_user2usergroup G 
							WHERE G.groupid = @id AND U.id=G.userid AND COALESCE(G.eb_del, 'F') = 'F' AND COALESCE(U.eb_del, 'F') = 'F';";

                Qry += EbConstraints.GetSelectQuery(EbConstraintKeyTypes.UserGroup, EbConnectionFactory.DataDB);
            }
            EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(Qry, new DbParameter[]
            {
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.id)
            });

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                _usersListAll.Add(new Eb_Users()
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = Convert.ToString(dr[1]),
                    Email = Convert.ToString(dr[2]),
                    Phone = Convert.ToString(dr[3])
                });
            }
            if (request.id > 0)
            {
                if (ds.Tables[1].Rows.Count > 0)
                {
                    _userGroupInfo.Add("id", Convert.ToInt32(ds.Tables[1].Rows[0][0]));
                    _userGroupInfo.Add("name", ds.Tables[1].Rows[0][1].ToString());
                    _userGroupInfo.Add("description", ds.Tables[1].Rows[0][2].ToString());
                    foreach (EbDataRow dr in ds.Tables[2].Rows)
                    {
                        _usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString() });
                    }

                    EbConstraints con = new EbConstraints(ds.Tables[3]);
                    foreach (var c in con.UgConstraints)
                    {
                        if (c.Value.Values.ElementAt(0).Value.Type == EbConstraintTypes.UserGroup_Ip)
                            _ipConsList.Add(new Eb_Constraints1 { Id = c.Key, Title = c.Value.Values.ElementAt(0).Value.GetValue(), Description = c.Value.Description });
                    }
                }
                else
                    _userGroupInfo.Add("id", 0);
            }
            else
                _userGroupInfo.Add("id", 0);

            return new GetManageUserGroupResponse()
            {
                SelectedUserGroupInfo = _userGroupInfo,
                UsersList = _usersList,
                IpConsList = _ipConsList,
                DtConsList = _dtConsList,
                UsersListAll = _usersListAll
            };

            //foreach (EbDataRow dr in ds.Tables[2].Rows)
            //{
            //    _ipConsList.Add(new Eb_Constraints1 { Id = Convert.ToInt32(dr["id"]), Title = dr["ip"].ToString(), Description = dr["description"].ToString() });
            //}
            //string[] days = { "Sun ", "Mon ", "Tue ", "Wed ", "Thu ", "Fri ", "Sat " };
            //foreach (EbDataRow dr in ds.Tables[3].Rows)
            //{
            //    int _type = Convert.ToInt32(dr["type"]);
            //    DateTime _start = Convert.ToDateTime(dr["start_datetime"]).ConvertFromUtc(request.Timezone);
            //    DateTime _end = Convert.ToDateTime(dr["end_datetime"]).ConvertFromUtc(request.Timezone);
            //    int _days = Convert.ToInt32(dr["days_coded"]);
            //    if (_type == 1)
            //    {
            //        string temp = "One Time - " + _start.ToString("dd-MM-yyyy HH:mm") + " to " + _end.ToString("dd-MM-yyyy HH:mm");
            //        _dtConsList.Add(new Eb_Constraints1 { Id = Convert.ToInt32(dr["id"]), Title = dr["title"].ToString(), Description = temp });
            //    }
            //    else if (_type == 2)
            //    {
            //        string temp = "Recurring - " + _start.ToString("HH:mm") + " to " + _end.ToString("HH:mm") + "<br>";
            //        for (int i = 0; i < 7; i++)
            //        {
            //            if ((Convert.ToInt32(Math.Pow(2, i)) & _days) > 0)
            //            {
            //                temp += days[i];
            //            }
            //        }
            //        _dtConsList.Add(new Eb_Constraints1 { Id = Convert.ToInt32(dr["id"]), Title = dr["title"].ToString(), Description = temp });
            //    }
            //}

            //using (var con = this.TenantDbFactory.DataDB.GetNewConnection())
            //{
            //	con.Open();
            //	string sql = "";
            //	if (request.id > 0)
            //	{
            //		sql = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
            //                        SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')";


            //		DbParameter[] parameters = { this.TenantDbFactory.DataDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

            //		var ds = this.TenantDbFactory.DataDB.DoQueries(sql, parameters);
            //		Dictionary<string, object> result = new Dictionary<string, object>();
            //		foreach (var dr in ds.Tables[0].Rows)
            //		{

            //			result.Add("name", dr[1].ToString());
            //			result.Add("description", dr[2].ToString());
            //		}
            //		List<int> users = new List<int>();
            //		if (ds.Tables.Count > 1)
            //		{
            //			foreach (EbDataRow dr in ds.Tables[1].Rows)
            //			{
            //				users.Add(Convert.ToInt32(dr[0]));
            //				result.Add(dr[0].ToString(), dr[1]);
            //			}
            //			result.Add("userslist", users);
            //		}
            //		resp.Data = result;
            //	}
            //	else
            //	{
            //		sql = "SELECT id,name FROM eb_usergroup";
            //		var dt = this.TenantDbFactory.DataDB.DoQueries(sql);

            //		Dictionary<string, object> returndata = new Dictionary<string, object>();
            //		foreach (EbDataRow dr in dt.Tables[0].Rows)
            //		{
            //			returndata[dr[0].ToString()] = dr[1].ToString();
            //		}
            //		resp.Data = returndata;
            //	}

            //}
            //return resp;
        }

        public SaveUserGroupResponse Post(SaveUserGroupRequest request)
        {
            SaveUserGroupResponse resp = new SaveUserGroupResponse() { id = 0 };
            //List<IpConstraint> IpConstr = JsonConvert.DeserializeObject<List<IpConstraint>>(request.IpConstraintNw);
            //List<DateTimeConstraint> DtConstr = JsonConvert.DeserializeObject<List<DateTimeConstraint>>(request.DtConstraintNw);
            //string sIpConstr = string.Empty;
            //string sDtConstr = string.Empty;

            //string _sIp = string.Empty;
            //string _sIpDesc = string.Empty;
            //string _sDtTitle = string.Empty;
            //string _sDtDesc = string.Empty;
            //string _sDtType = string.Empty;
            //string _sDtStart = string.Empty;
            //string _sDtEnd = string.Empty;
            //string _sDtDays = string.Empty;

            //foreach (IpConstraint _ipc in IpConstr)
            //{
            //    _sIp += _ipc.Ip.Replace(" ", "_") + ",";
            //    _sIpDesc += _ipc.Description.Replace(" ", "_") + ",";
            //    //sIpConstr += _ipc.Ip.Replace(" ", "_") + "," + _ipc.Description.Replace(" ", "_") + ",,";
            //}
            //if (_sIp.Length > 0)
            //{
            //    sIpConstr = _sIp.Substring(0, _sIp.Length - 1) + "$$" + _sIpDesc.Substring(0, _sIpDesc.Length - 1);
            //}

            //foreach (DateTimeConstraint _dtc in DtConstr)
            //{
            //    if (_dtc.Type == 1)//One Time
            //    {
            //        _dtc.Start = Convert.ToDateTime(DateTime.ParseExact(_dtc.Start, "dd-MM-yyyy HH:mm", CultureInfo.InvariantCulture)).ConvertToUtc(request.UsersTimezone).ToString("yyyy-MM-dd HH:mm:ss");
            //        _dtc.End = Convert.ToDateTime(DateTime.ParseExact(_dtc.End, "dd-MM-yyyy HH:mm", CultureInfo.InvariantCulture)).ConvertToUtc(request.UsersTimezone).ToString("yyyy-MM-dd HH:mm:ss");
            //    }
            //    else if (_dtc.Type == 2)//recurring
            //    {
            //        _dtc.Start = Convert.ToDateTime(DateTime.ParseExact(_dtc.Start, "HH:mm", CultureInfo.InvariantCulture)).ConvertToUtc(request.UsersTimezone).ToString("yyyy-MM-dd HH:mm:ss");
            //        _dtc.End = Convert.ToDateTime(DateTime.ParseExact(_dtc.End, "HH:mm", CultureInfo.InvariantCulture)).ConvertToUtc(request.UsersTimezone).ToString("yyyy-MM-dd HH:mm:ss");
            //    }
            //    _sDtTitle += _dtc.Title.Replace(" ", "_") + ",";
            //    _sDtDesc += _dtc.Description.Replace(" ", "_") + ",";
            //    _sDtType += _dtc.Type + ",";
            //    _sDtStart += _dtc.Start + ",";
            //    _sDtEnd += _dtc.End + ",";
            //    _sDtDays += _dtc.DaysCoded + ",";
            //    //sDtConstr += _dtc.Title.Replace(" ", "_") + "," + _dtc.Description.Replace(" ", "_") + "," + _dtc.Type + "," + _dtc.Start + "," + _dtc.End + "," + _dtc.DaysCoded + ",,";
            //}
            //if (_sDtTitle.Length > 0)
            //{
            //    sDtConstr = _sDtTitle.Substring(0, _sDtTitle.Length - 1) + "$$" + _sDtDesc.Substring(0, _sDtDesc.Length - 1) + "$$" + _sDtType.Substring(0, _sDtType.Length - 1) + "$$" + _sDtStart.Substring(0, _sDtStart.Length - 1) + "$$" + _sDtEnd.Substring(0, _sDtEnd.Length - 1) + "$$" + _sDtDays.Substring(0, _sDtDays.Length - 1);
            //}
            List<string> OldUserIds = new List<string>();
            try
            {
                EbDataTable edt = this.EbConnectionFactory.DataDB.DoQuery(@"SELECT userid  FROM eb_user2usergroup WHERE groupid = @uid AND eb_del = 'F';",
                            new DbParameter[]{
                this.EbConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.Int32, request.Id)
                            });
                if (edt.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in edt.Rows)
                    {
                        OldUserIds.Add(dr[0].ToString());
                    }
                }

            }
            catch (Exception e1)
            {
                Console.WriteLine("Error while fetch user groupid from eb_user2usergroup " + e1.Message + e1.StackTrace);
            }
            EbDataTable d = this.EbConnectionFactory.DataDB.DoQuery($"SELECT id FROM eb_usergroup WHERE LOWER(name) LIKE LOWER(@ugname) {(request.Id > 0 ? "AND id <> " + request.Id : "")};",
                new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("ugname", EbDbTypes.String, request.Name) });
            if (d.Rows.Count > 0)
            {
                resp.id = -1;
                return resp;
            }

            List<IpConstraint> IpConstr = JsonConvert.DeserializeObject<List<IpConstraint>>(request.IpConstraintNw);
            EbConstraints consObj = new EbConstraints();
            consObj.SetConstraintObject(IpConstr);
            request.IpConstraintNw = consObj.GetDataAsString();

            List<DbParameter> parameters = new List<DbParameter>
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
                    this.EbConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String, request.Name),
                    this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Description),
                    this.EbConnectionFactory.DataDB.GetNewParameter("users", EbDbTypes.String,(request.Users != string.Empty? request.Users : string.Empty)),
                    this.EbConnectionFactory.DataDB.GetNewParameter("constraints_add", EbDbTypes.String, request.IpConstraintNw),
                    this.EbConnectionFactory.DataDB.GetNewParameter("constraints_del", EbDbTypes.String, request.IpConstraintOld)
                };
            if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
            {
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewOutParameter("out_gid", EbDbTypes.Int32));
                EbDataTable dt = EbConnectionFactory.DataDB.DoProcedure(this.EbConnectionFactory.DataDB.EB_SAVEUSERGROUP_QUERY, parameters.ToArray());
                if (dt.Rows.Count > 0)
                    resp.id = Convert.ToInt32(dt.Rows[0][0]);
            }
            else
            {
                EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(this.EbConnectionFactory.DataDB.EB_SAVEUSERGROUP_QUERY, parameters.ToArray());
                resp.id = Convert.ToInt32(dt.Tables[0].Rows[0][0]);
            }
            try
            {
                this.MessageProducer3.Publish(new SaveUserGroupMqRequest
                {
                    BToken = this.ServerEventClient.BearerToken,
                    RToken = this.ServerEventClient.RefreshToken,
                    SolnId = request.SolnId,
                    UserId = request.Id,
                    NewUserGroups = request.Users.Split(",").ToList(),
                    OldUserGroups = OldUserIds,
                    WhichConsole = "uc"
                });

            }
            catch (Exception ex)
            {
                Console.WriteLine("save user - message queue" + ex.Message + ex.StackTrace);
            }
            return resp;
        }

        //----MANAGE ROLES START---------------------------------------
        public GetManageRolesResponse Any(GetManageRolesRequest request)
        {
            GetManageRolesResponse resp = null;
            try
            {
                string query = this.EbConnectionFactory.DataDB.EB_GETMANAGEROLESRESPONSE_QUERY;
                int Set1_QryCount = 6;// Number of queries in EB_GETMANAGEROLESRESPONSE_QUERY
                if (request.id > 0)
                    query += this.EbConnectionFactory.DataDB.EB_GETMANAGEROLESRESPONSE_QUERY_EXTENDED;

                DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };
                var ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters);
                ApplicationCollection _applicationCollection = null;
                List<Eb_RoleObject> _roleList = new List<Eb_RoleObject>();
                List<Eb_RoleToRole> _r2rList = new List<Eb_RoleToRole>();
                List<Eb_Location> _location = new List<Eb_Location>();
                List<Eb_Users> _usersListAll = new List<Eb_Users>();

                if (ds.Tables.Count > 0)
                {
                    //PROCESSED RESULT
                    _applicationCollection = new ApplicationCollection(ds.Tables[0], ds.Tables[1]);
                    //---------------
                    foreach (EbDataRow dr in ds.Tables[2].Rows)
                    {
                        _roleList.Add(new Eb_RoleObject()
                        {
                            Id = Convert.ToInt32(dr[0]),
                            Name = dr[1].ToString(),
                            Description = dr[2].ToString(),
                            App_Id = Convert.ToInt32(dr[3]),
                            Is_Anonymous = (dr[4].ToString() == "T") ? true : false,
                            Is_System = false
                        });
                    }
                    foreach (EbDataRow dr in ds.Tables[3].Rows)
                    {
                        _r2rList.Add(new Eb_RoleToRole()
                        {
                            Id = Convert.ToInt32(dr[0]),
                            Dominant = Convert.ToInt32(dr[1]),
                            Dependent = Convert.ToInt32(dr[2])
                        });
                    }
                    foreach (EbDataRow dr in ds.Tables[4].Rows)
                    {
                        _location.Add(new Eb_Location()
                        {
                            Id = Convert.ToInt32(dr[0]),
                            LongName = dr[1].ToString(),
                            ShortName = dr[2].ToString()
                        });
                    }
                    foreach (EbDataRow dr in ds.Tables[5].Rows)
                    {
                        _usersListAll.Add(new Eb_Users()
                        {
                            Id = Convert.ToInt32(dr[0]),
                            Name = Convert.ToString(dr[1]),
                            Email = Convert.ToString(dr[2]),
                            Phone = Convert.ToString(dr[3])
                        });
                    }
                }
                Dictionary<string, object> RoleInfo = new Dictionary<string, object>();
                List<string> Permission = new List<string>();
                List<Eb_Users> _usersList = new List<Eb_Users>();

                if (ds.Tables.Count > Set1_QryCount)
                {
                    if (ds.Tables[Set1_QryCount].Rows.Count == 0)
                        throw new Exception("Role not found");
                    RoleInfo.Add("RoleName", ds.Tables[Set1_QryCount].Rows[0][0].ToString());
                    RoleInfo.Add("RoleDescription", ds.Tables[Set1_QryCount].Rows[0][2].ToString());
                    RoleInfo.Add("IsAnonymous", (ds.Tables[Set1_QryCount].Rows[0][3].ToString() == "T") ? true : false);
                    RoleInfo.Add("IsPrimary", (ds.Tables[Set1_QryCount].Rows[0][4].ToString() == "T") ? true : false);
                    RoleInfo.Add("AppId", Convert.ToInt32(ds.Tables[Set1_QryCount].Rows[0][5]));
                    RoleInfo.Add("AppName", ds.Tables[Set1_QryCount].Rows[0][6].ToString());
                    RoleInfo.Add("AppDescription", ds.Tables[Set1_QryCount].Rows[0][7].ToString());
                    foreach (var dr in ds.Tables[Set1_QryCount + 1].Rows)
                        Permission.Add(dr[0].ToString());
                    foreach (EbDataRow dr in ds.Tables[Set1_QryCount + 2].Rows)
                    {
                        _usersList.Add(new Eb_Users()
                        {
                            Id = Convert.ToInt32(dr[0]),
                            Name = Convert.ToString(dr[1]),
                            Email = Convert.ToString(dr[2]),
                            Phone = Convert.ToString(dr[3]),
                            Role2User_Id = Convert.ToInt32(dr[4])
                        });
                    }
                    string temp_locs = string.Empty;
                    foreach (var dr in ds.Tables[Set1_QryCount + 3].Rows)
                        temp_locs += dr[0].ToString() + ",";
                    RoleInfo.Add("LocationIds", string.IsNullOrEmpty(temp_locs) ? "" : temp_locs.Substring(0, temp_locs.Length - 1));
                }
                resp = new GetManageRolesResponse()
                {
                    ApplicationCollection = _applicationCollection,
                    SelectedRoleInfo = RoleInfo,
                    PermissionList = Permission,
                    RoleList = _roleList,
                    Role2RoleList = _r2rList,
                    UsersList = _usersList,
                    LocationList = _location,
                    UsersListAll = _usersListAll
                };
            }
            catch (Exception e)
            {
                Console.WriteLine("ManageRolesRequest___999 " + e.ToString());
            }
            return resp;
        }

        public GetUserDetailsResponse Any(GetUserDetailsRequest request)
        {
            string query = "SELECT id, fullname, email, phnoprimary FROM eb_users WHERE statusid = 0 AND COALESCE(eb_del, 'F') = 'F' AND id > 1 ORDER BY fullname, email, phnoprimary LIMIT 5000;";
            //DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, request.SearchText) };

            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
            List<Eb_Users> _usersList = new List<Eb_Users>();

            foreach (EbDataRow dr in dt.Rows)
            {
                _usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString() });
            }
            return new GetUserDetailsResponse() { UserList = _usersList };
        }

        public SaveRoleResponse Post(SaveRoleRequest request)
        {

            List<string> OldPermission = new List<string>();
            List<int> OldUsers = new List<int>();
            SaveRoleResponse resp = new SaveRoleResponse() { id = 0 };
            int role_id = Convert.ToInt32(request.Colvalues["roleid"]);

            string query = "SELECT permissionname FROM eb_role2permission WHERE role_id = :id AND COALESCE(eb_del, 'F') = 'F'; " +
                "SELECT A.id FROM eb_users A, eb_role2user B WHERE A.id = B.user_id AND COALESCE(A.eb_del, 'F') = 'F' AND COALESCE(B.eb_del, 'F') = 'F' AND B.role_id = :id; " +
                "SELECT id FROM eb_roles WHERE LOWER(role_name) LIKE LOWER(@roleName) AND id <> @id; ";

            EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(query, new DbParameter[]
            {
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, role_id),
                this.EbConnectionFactory.DataDB.GetNewParameter("roleName", EbDbTypes.String, request.Colvalues["role_name"])
            });

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                if (!OldPermission.Contains(dr[0].ToString()))
                    OldPermission.Add(dr[0].ToString());
            }
            foreach (EbDataRow dr in ds.Tables[1].Rows)
            {
                if (int.TryParse(dr[0].ToString(), out int uid) && !OldUsers.Contains(uid))
                    OldUsers.Add(uid);
            }

            if (ds.Tables[2].Rows.Count > 0)
            {
                resp.id = -1;
                return resp;
            }

            List<DbParameter> parameters = new List<DbParameter>
            {
                this.EbConnectionFactory.DataDB.GetNewParameter("role_id", EbDbTypes.Int32, role_id),
                this.EbConnectionFactory.DataDB.GetNewParameter("applicationid", EbDbTypes.Int32, Convert.ToInt32(request.Colvalues["applicationid"])),
                this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("role_name", EbDbTypes.String, request.Colvalues["role_name"]),
                this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["Description"]),
                this.EbConnectionFactory.DataDB.GetNewParameter("is_anonym", EbDbTypes.String, request.Colvalues["IsAnonymous"]),
                this.EbConnectionFactory.DataDB.GetNewParameter("is_primary", EbDbTypes.String, request.Colvalues["IsPrimary"]),
                this.EbConnectionFactory.DataDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"] : string.Empty),
                this.EbConnectionFactory.DataDB.GetNewParameter("dependants", EbDbTypes.String, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"] : string.Empty),
                this.EbConnectionFactory.DataDB.GetNewParameter("permission", EbDbTypes.String , (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"]: string.Empty),
                this.EbConnectionFactory.DataDB.GetNewParameter("locations", EbDbTypes.String , request.Colvalues["locations"].ToString())
            };

            if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
            {
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewOutParameter("out_r", EbDbTypes.Int32));
                EbDataTable dt = EbConnectionFactory.DataDB.DoProcedure(this.EbConnectionFactory.DataDB.EB_SAVEROLES_QUERY, parameters.ToArray());
                if (dt.Rows.Count > 0)
                    resp.id = Convert.ToInt32(dt.Rows[0][0]);
            }
            else
            {
                EbDataTable ds2 = this.EbConnectionFactory.DataDB.DoQuery(this.EbConnectionFactory.DataDB.EB_SAVEROLES_QUERY, parameters.ToArray());
                resp.id = Convert.ToInt32(ds2.Rows[0][0]);
            }

            try
            {
                UpdateUserIfPermissionChanged(request, OldPermission, OldUsers);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in update user obj process/mq_call: " + ex.Message + ex.StackTrace);
            }

            return resp;
        }

        private void UpdateUserIfPermissionChanged(SaveRoleRequest request, List<string> OldPermission, List<int> OldUsers)
        {
            List<string> NewPermission = string.IsNullOrWhiteSpace(request.Colvalues["permission"]?.ToString()) ?
                new List<string>() : request.Colvalues["permission"].ToString().Split(',').ToList();
            List<int> NewUsers = string.IsNullOrWhiteSpace(request.Colvalues["users"]?.ToString()) ?
                new List<int>() : request.Colvalues["users"].ToString().Split(',').Select(e => int.Parse(e)).ToList();

            List<int> outUsers = new List<int>();

            if (NewPermission.Except(OldPermission).Count() > 0 || OldPermission.Except(NewPermission).Count() > 0)
            {
                NewUsers.AddRange(OldUsers);
                outUsers.AddRange(NewUsers.Distinct().ToList());
            }
            else
            {
                if (NewUsers.Except(OldUsers).Count() > 0)
                    outUsers.AddRange(NewUsers.Except(OldUsers).ToList());

                if (OldUsers.Except(NewUsers).Count() > 0)
                    outUsers.AddRange(OldUsers.Except(NewUsers).ToList());
            }

            if (outUsers.Count > 0)
            {
                this.MessageProducer3.Publish(new SaveRoleMqRequest
                {
                    BToken = this.ServerEventClient.BearerToken,
                    RToken = this.ServerEventClient.RefreshToken,
                    SolnId = request.SolnId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    WhichConsole = request.WhichConsole,
                    UserIdsToUpdate = outUsers
                });
            }
        }

        //--API KEY GENERATION
        public object Get(GenerateAPIKey request)
        {
            GenerateAPIKeyResponse resp = new GenerateAPIKeyResponse();
            //var authProvider = (ApiKeyAuthProvider)AuthenticateService.GetAuthProvider(ApiKeyAuthProvider.Name);

            try
            {
                var auth_api = (ApiKeyAuthProvider)AuthenticateService.GetAuthProvider(ApiKeyAuthProvider.Name);

                var authRepo = TryResolve<IManageApiKeys>();

                resp.APIKeys = auth_api.GenerateNewApiKeys(request.UserAuthId);

                authRepo.StoreAll(resp.APIKeys);
            }
            catch (Exception e)
            {
                resp.ResponseStatus = new ResponseStatus()
                {
                    ErrorCode = "APIError",
                    Message = e.Message,
                    StackTrace = e.StackTrace
                };
            }

            return resp;
        }

        public LoginActivityResponse Post(LoginActivityRequest LaReq)
        {
            LoginActivityResponse Lar = new LoginActivityResponse();
            try
            {

                if (LaReq.Alluser)
                {
                    var sql = EbConnectionFactory.DataDB.EB_LOGIN_ACTIVITY_ALL_USERS;
                    DbParameter[] parameters = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("islg", EbDbTypes.String, "F")
                    };
                    EbDataTable dt2 = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
                    if (dt2.Rows.Count > 0)
                    {
                        for (int i = 0; i < dt2.Rows.Count; i++)
                        {

                            DateTime SinDateUTC = (DateTime)dt2.Rows[i][3];
                            DateTime SinDate = (SinDateUTC.ConvertFromUtc(LaReq.UserObject.Preference.TimeZone));
                            dt2.Rows[i][3] = SinDate;
                            dt2.Rows[i][4] = SinDate.ToString("hh:mm:ss tt");
                            DateTime SoutDateUTC = (DateTime)dt2.Rows[i][5];
                            if (!(SoutDateUTC == DateTime.MinValue))
                            {
                                DateTime SoutDate = (SoutDateUTC.ConvertFromUtc(LaReq.UserObject.Preference.TimeZone));
                                dt2.Rows[i][5] = SoutDate;
                                dt2.Rows[i][6] = SoutDate.ToString("hh:mm:ss tt");
                            }
                            if (!(dt2.Rows[i][7].ToString() == ""))
                            {
                                string[] afterSplit = (dt2.Rows[i][7]).ToString().Split(':');
                                int lgth = afterSplit.Length - 1;
                                for (int j = lgth; j >= 0; j--)
                                {
                                    if (j == lgth)
                                        afterSplit[j] = afterSplit[j] + "s";
                                    if (j == lgth - 1)
                                        afterSplit[j] = afterSplit[j] + "m :";
                                    if (j == lgth - 2)
                                        afterSplit[j] = afterSplit[j] + "h :";

                                }

                                dt2.Rows[i][7] = string.Concat(afterSplit); ;
                            }
                            string usrtyp = "";
                            DeviceInfo Dci = JsonConvert.DeserializeObject<DeviceInfo>(dt2.Rows[i][1].ToString());
                            if (!(Dci == null))
                            {
                                if (Dci.WC == "uc")
                                {
                                    usrtyp = "User";
                                }
                                else
                                    if (Dci.WC == "dc")
                                {
                                    usrtyp = "Developer";
                                }
                            }
                            dt2.Rows[i][1] = usrtyp;

                        }

                    }
                    Lar._data = dt2;
                }
                else
                {
                    var sql1 = EbConnectionFactory.DataDB.EB_LOGIN_ACTIVITY_USERS;
                    DbParameter[] parameters1 = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("islg", EbDbTypes.String, "F"),
                    this.EbConnectionFactory.DataDB.GetNewParameter("usrid", EbDbTypes.Int32, LaReq.TargetUser)
                    };
                    EbDataTable dt3 = this.EbConnectionFactory.DataDB.DoQuery(sql1, parameters1);
                    if (dt3.Rows.Count > 0)
                    {
                        for (int i = 0; i < dt3.Rows.Count; i++)
                        {

                            DateTime SinDateUTC = (DateTime)dt3.Rows[i][1];
                            DateTime SinDate = (SinDateUTC.ConvertFromUtc(LaReq.UserObject.Preference.TimeZone));
                            dt3.Rows[i][1] = SinDate;
                            dt3.Rows[i][2] = SinDate.ToString("hh:mm:ss tt");
                            DateTime SoutDateUTC = (DateTime)dt3.Rows[i][3];
                            if (!(SoutDateUTC == DateTime.MinValue))
                            {
                                DateTime SoutDate = (SoutDateUTC.ConvertFromUtc(LaReq.UserObject.Preference.TimeZone));
                                dt3.Rows[i][3] = SoutDate;
                                dt3.Rows[i][4] = SoutDate.ToString("hh:mm:ss tt");
                            }

                        }

                    }
                    Lar._data = dt3;
                }


                return Lar;

            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                Lar.ErrMsg = "Unexpected error occurred";
            }

            return Lar;
        }

        public GetUserTypesResponse Get(GetUserTypesRequest request)
        {
            GetUserTypesResponse response = new GetUserTypesResponse();
            string query = "SELECT id, name FROM eb_user_types WHERE COALESCE(eb_del, 'F') = 'F'";
            if (request.Id > 0)
                query += String.Format(" AND id = {0} ", request.Id);
            query += " ORDER BY id";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
            List<EbProfileUserType> userTypes = new List<EbProfileUserType>();
            foreach (EbDataRow _dr in dt.Rows)
            {
                EbProfileUserType _type = new EbProfileUserType
                {
                    Id = Convert.ToInt32(_dr["id"]),
                    Name = _dr["name"].ToString()
                };
                userTypes.Add(_type);
            }
            response.UserTypes = userTypes;

            return response;
        }

        public UpdateUserTypeResponse Post(UpdateUserTypeRequset request)
        {
            UpdateUserTypeResponse response = new UpdateUserTypeResponse { };
            try
            {
                string query;
                DbParameter[] parameters = new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,request.Name),
                     this.EbConnectionFactory.DataDB.GetNewParameter("by", EbDbTypes.Int32, request.UserId),
                      this.EbConnectionFactory.DataDB.GetNewParameter("at", EbDbTypes.DateTime, DateTime.Now),
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                };

                if (request.Id > 0)
                {
                    query = @"UPDATE eb_user_types SET name = @name , eb_lastmodified_by = @by, eb_lastmodified_at = @at WHERE id = @id;";
                }
                else
                {
                    query = string.Format("INSERT INTO eb_user_types(name, eb_created_by, eb_created_at) VALUES(@name, @by, @at)");
                }

                int c = this.EbConnectionFactory.DataDB.DoNonQuery(query, parameters);
                if (c > 0)
                    response.Status = true;
                else
                    response.Status = false;
            }
            catch (Exception e)
            {
                response.Status = false;
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return response;
        }
    }
}
