using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class TenantUserServices : EbBaseService
    {
        public TenantUserServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        //public CreateLocationConfigResponse Post(CreateLocationConfigRequest request)
        //{
        //    using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //    {
        //        con.Open();
        //        List<EbLocationConfig> list = request.ConfString;
        //        StringBuilder query1 = new StringBuilder();
        //        query1.Append(@"INSERT INTO eb_location_config (keys,isrequired,keytype,eb_del) VALUES");
        //        List<DbParameter> parameters1 = new List<DbParameter>();
        //        string keys = ":key", isrequired = ":isrequired", type = ":type";
        //        int count = 0;
        //        int InsertCount = 0;
        //        for (int i = 0; i < list.Count(); i++)
        //            if (list[i].KeyId == null)
        //            {
        //                query1.Append("( " + (keys + count) + "," + (isrequired + count) + ","+ (type + count)+",'F'),");
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":key" + count, EbDbTypes.String, list[i].Name));
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":isrequired" + count, EbDbTypes.String, list[i].Isrequired));
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":type" + count, EbDbTypes.String, list[i].Type));
        //                count++;
        //                list.Remove(list[i]);
        //                i--;
        //                InsertCount++;
        //            }
        //        query1.Length--;
        //        query1.Append(";");
        //        int dt1 = 0;
        //        if (InsertCount > 0)
        //            dt1 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query1.ToString(), parameters1.ToArray());
        //        if (list.Count() == 0)
        //            return new CreateLocationConfigResponse { };
        //        StringBuilder query2 = new StringBuilder();
        //        query2.Append(@"UPDATE eb_location_config AS EL SET keys = L.keys , isrequired =L.isrequired FROM (VALUES");
        //        List<DbParameter> parameters2 = new List<DbParameter>();
        //        string kname = ":kname", kreq = ":kreq", kid = ":kid",ktype = ":ktype";
        //        count = 0;
        //        foreach (var obj in list)
        //        {
        //            query2.Append("(" + (kname + count) + "," + (kreq + count) + "," + (kid + count) +","+ (ktype + count) + "),");
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kname + count), EbDbTypes.String, obj.Name));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kreq + count), EbDbTypes.String, obj.Isrequired));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kid + count), EbDbTypes.Int32, Convert.ToInt32(obj.KeyId)));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((ktype + count), EbDbTypes.String, obj.Type));
        //            count++;
        //        }
        //        query2.Length--;
        //        query2.Append(") AS L(keys, isrequired,kid,ktype) WHERE L.kid = EL.id;");
        //        var dt2 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters2.ToArray());
        //        return new CreateLocationConfigResponse { };
        //    }
        //}


        public CreateLocationConfigResponse Post(CreateLocationConfigRequest request)
        {
            using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                EbLocationConfig conf = request.Conf;
                string query = @"INSERT INTO eb_location_config (keys,isrequired,keytype,eb_del) VALUES(:keys,:isrequired,:type,'F') RETURNING id";
                string query2 = @"UPDATE eb_location_config SET keys = :keys ,isrequired = :isrequired , keytype = :type WHERE id = :keyid";
                string exeq = "";

                List<DbParameter> parameters = new List<DbParameter>();
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":keys", EbDbTypes.String, conf.Name));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":isrequired", EbDbTypes.String, conf.Isrequired));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":type", EbDbTypes.String, conf.Type));

                if (conf.KeyId != null)
                {
                    exeq = query2;
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":keyid", EbDbTypes.String, conf.KeyId));
                }
                else
                    exeq = query;

                var ds = this.EbConnectionFactory.ObjectsDB.DoQuery(exeq, parameters.ToArray());

                return new CreateLocationConfigResponse { Id = Convert.ToInt32(ds.Rows[0][0]) };
            }
        }

        public SaveLocationMetaResponse Post(SaveLocationMetaRequest request)
        {
            using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                List<DbParameter> parameters = new List<DbParameter>();
                int result = 0;
                var query1 = "INSERT INTO eb_locations(longname,shortname,image,meta_json) VALUES(:lname,:sname,:img,:meta) RETURNING id;";
                var query2 = "UPDATE eb_locations SET longname= :lname, shortname = :sname, image = :img, meta_json = :meta WHERE id = :lid;";
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":lname", EbDbTypes.String, request.Longname));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":sname", EbDbTypes.String, request.Shortname));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":img", EbDbTypes.String, request.Img));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":meta", EbDbTypes.String, request.ConfMeta));
                if (request.Locid > 0)
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":lid", EbDbTypes.Int32, request.Locid));
                    var t = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters.ToArray());
                    result = t;
                }
                else
                {
                    var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(query1.ToString(), parameters.ToArray());
                    result = Convert.ToInt32(dt.Rows[0][0]);
                }
                this.Post(new UpdateSolutionRequest() { TenantAccountId = request.TenantAccountId, UserId = request.UserId, Token = request.Token });
                return new SaveLocationMetaResponse { Id =  result};
            }
        }

        public DeleteLocResponse Post(DeleteLocRequest request)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            string query = "UPDATE eb_location_config SET eb_del = 'T' WHERE id=:id RETURNING id";
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id));
            var dt = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query.ToString(), parameters.ToArray());
            return new DeleteLocResponse {id=(dt==1)?request.Id:0 };
        }

        public UpdateSolutionResponse Post(UpdateSolutionRequest req)
        {
            var _infraService = base.ResolveService<InfraServices>();
            GetSolutioInfoResponse res = (GetSolutioInfoResponse)_infraService.Get(new GetSolutioInfoRequest { IsolutionId = req.TenantAccountId });
            EbSolutionsWrapper wrap_sol = res.Data;
            LocationInfoResponse Loc = this.Get(new LocationInfoRequest());
            Eb_Solution sol_Obj = new Eb_Solution
            {
                InternalSolutionID = req.TenantAccountId,
                ExternalSolutionID = wrap_sol.EsolutionId.ToString(),
                DateCreated = wrap_sol.DateCreated.ToString(),
                Description = wrap_sol.Description.ToString(),
                LocationCollection = Loc.Locations,
                NumberOfUsers = 2,
                SolutionName = wrap_sol.SolutionName.ToString()
            };

            this.Redis.Set<Eb_Solution>(String.Format("solution_{0}", req.TenantAccountId), sol_Obj);
            var x = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", req.TenantAccountId));

            return new UpdateSolutionResponse { };
        }

        public LocationInfoResponse Get(LocationInfoRequest req)
        {
            List<EbLocationConfig> Conf = new List<EbLocationConfig>();
            Dictionary<int,EbSolutionLocation> locs = new Dictionary<int,EbSolutionLocation>();

            string query = "SELECT * FROM eb_location_config WHERE eb_del = 'F' ORDER BY id; SELECT * FROM eb_locations;";
            EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

            foreach (EbDataRow r in dt.Tables[0].Rows)
            {
                Conf.Add(new EbLocationConfig
                {
                    Name = r[1].ToString(),
                    Isrequired = (r[2].ToString() == "T") ? "true" : "false",
                    KeyId = r[0].ToString(),
                    Type = r[3].ToString()
                });
            }

            foreach (var r in dt.Tables[1].Rows)
            {
                locs.Add(Convert.ToInt32(r[0]),new EbSolutionLocation
                {
                    LocId = Convert.ToInt32(r[0]),
                    ShortName = r[1].ToString(),
                    LongName = r[2].ToString(),
                    Img = r[3].ToString(),
                    Meta = JsonConvert.DeserializeObject<Dictionary<string, string>>(r[4].ToString())
                });
            }
            return new LocationInfoResponse { Locations = locs, Config = Conf };
        }

        //private string GeneratePassword()
        //{
        //    string strPwdchar = "abcdefghijklmnopqrstuvwxyz0123456789#+@&$ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        //    string strPwd = "";
        //    Random rnd = new Random();
        //    for (int i = 0; i <= 7; i++)
        //    {
        //        int iRandom = rnd.Next(0, strPwdchar.Length - 1);
        //        strPwd += strPwdchar.Substring(iRandom, 1);
        //    }
        //    return strPwd;
        //}

        //    public CreateUserResponse Post(CreateUserRequest request)
        //    {
        //        CreateUserResponse resp;
        //        string sql = "";
        //        using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //        {
        //            con.Open();
        //string password = "";

        //            if (request.Id > 0)
        //            {
        //	sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@firstname,@email,@pwd,@roles,@group);";

        //}
        //            else
        //            {
        //	password = string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) ? GeneratePassword() : (request.Colvalues["pwd"].ToString() + request.Colvalues["email"].ToString()).ToMD5Hash();
        //	sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@firstname,@email,@pwd,@roles,@group);";

        //}
        //int[] emptyarr = new int[] { };
        //            DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("firstname", EbDbTypes.String, request.Colvalues["firstname"]),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.Colvalues["email"]),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("roles", EbDbTypes.String,(request.Colvalues["roles"].ToString() != string.Empty? request.Colvalues["roles"] : string.Empty )),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("group", EbDbTypes.String,(request.Colvalues["group"].ToString() != string.Empty? request.Colvalues["group"] : string.Empty )),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("pwd", EbDbTypes.String,password),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)};

        //            EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //            if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
        //            {
        //                using (var service = base.ResolveService<EmailService>())
        //                {
        //                  //  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
        //                }
        //            }
        //            resp = new CreateUserResponse
        //            {
        //                id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

        //            };
        //        } 
        //        return resp;
        //    }

        //     public GetUserEditResponse Any(GetUserEditRequest request)
        //     {
        //         GetUserEditResponse resp = new GetUserEditResponse();
        //string sql = null;
        //if (request.Id > 0)
        //{
        //	sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
        //                     SELECT id, name,description FROM eb_usergroup ORDER BY name;
        //			SELECT firstname,email FROM eb_users WHERE id = @id;
        //			SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = 'F';
        //			SELECT groupid FROM eb_user2usergroup WHERE userid = @id AND eb_del = 'F';";
        //}
        //else
        //{
        //	sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
        //                     SELECT id, name,description FROM eb_usergroup ORDER BY name";
        //}

        //DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.Id) };
        //var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //resp.Roles = new List<EbRole>();
        //foreach (var dr in ds.Tables[0].Rows)
        //{ 						
        //	resp.Roles.Add(new EbRole
        //	{
        //		Id = Convert.ToInt32(dr[0]),
        //		Name = dr[1].ToString(),
        //		Description = dr[2].ToString()
        //	});
        //}

        //resp.EbUserGroups = new List<EbUserGroups>();
        //foreach (var dr in ds.Tables[1].Rows)
        //{
        //	resp.EbUserGroups.Add(new EbUserGroups
        //	{
        //		Id = Convert.ToInt32(dr[0]),
        //		Name = dr[1].ToString(),
        //		Description = dr[2].ToString()

        //	});
        //}

        //if (request.Id > 0)
        //{
        //	resp.UserData = new Dictionary<string, object>();
        //	foreach (var dr in ds.Tables[2].Rows)
        //	{
        //		resp.UserData.Add("name", dr[0].ToString());
        //		resp.UserData.Add("email", dr[1].ToString());
        //	}

        //	resp.UserRoles = new List<int>();
        //	foreach (var dr in ds.Tables[3].Rows)
        //		resp.UserRoles.Add(Convert.ToInt32(dr[0]));

        //	resp.UserGroups = new List<int>();
        //	foreach (var dr in ds.Tables[4].Rows)
        //		resp.UserGroups.Add(Convert.ToInt32(dr[0]));
        //}

        //         return resp;
        //     }

        //     public GetUserRolesResponse Any(GetUserRolesRequest request)
        //     {
        //         GetUserRolesResponse resp = new GetUserRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = string.Empty;
        //             if (request.id > 0)
        //                 sql = @"SELECT id, role_name, description FROM eb_roles;
        //                          SELECT id, role_name, description FROM eb_roles WHERE id IN(SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = 'F')";
        //             else
        //                 sql = "SELECT id,role_name, description FROM eb_roles";

        //             DbParameter[] parameters = {
        //                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id)};

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> subroles = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //		List<string> list = new List<string>();
        //		list.Add(dr[1].ToString());
        //		list.Add(dr[2].ToString());
        //                 returndata[dr[0].ToString()] = list;
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     subroles.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("roles", subroles);
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }


        //     public RBACRolesResponse Post(RBACRolesRequest request)
        //     {
        //         RBACRolesResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @users, @dependants,@permission );";
        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
        //             int[] emptyarr = new int[] { };
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_id", EbDbTypes.Int32, request.Colvalues["roleid"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["Description"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_name", EbDbTypes.String, request.Colvalues["role_name"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Colvalues["applicationid"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, request.UserId));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("permission", EbDbTypes.String, (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dependants", EbDbTypes.String, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"] : string.Empty));

        //             resp = new RBACRolesResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     }

        //     public CreateUserGroupResponse Post(CreateUserGroupRequest request)
        //     {
        //         CreateUserGroupResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "";
        //             if (request.Id > 0)
        //             {
        //                 sql = @"UPDATE eb_usergroup SET name = @name,description = @description WHERE id = @id;
        //                                 INSERT INTO eb_user2usergroup(userid,groupid) SELECT uid,@id FROM UNNEST(array(SELECT unnest(@users) except 
        //                                     SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')))) as uid;
        //                                 UPDATE eb_user2usergroup SET eb_del = 'T' WHERE userid IN(
        //                                     SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')) except SELECT UNNEST(@users));";
        //             }
        //             else
        //             {
        //                 sql = @"INSERT INTO eb_usergroup (name,description) VALUES (@name,@description) RETURNING id;
        //                                    INSERT INTO eb_user2usergroup (userid,groupid) SELECT id, (CURRVAL('eb_usergroup_id_seq')) FROM UNNEST(@users) AS id";
        //             }

        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
        //             int[] emptyarr = new int[] { };
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.Colvalues["groupname"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["description"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["userlist"].ToString() != string.Empty) ? request.Colvalues["userlist"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id));
        //             resp = new CreateUserGroupResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     } //user group creation

        //     public UserPreferenceResponse Post(UserPreferenceRequest request)
        //     {
        //         UserPreferenceResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_users SET locale=@locale,timezone=@timezone,dateformat=@dateformat,numformat=@numformat,timezonefull=@timezonefull WHERE id=@id");
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("locale", EbDbTypes.String, request.Colvalues["locale"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("timezone", EbDbTypes.String, request.Colvalues["timecode"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateformat", EbDbTypes.String, request.Colvalues["dateformat"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("numformat", EbDbTypes.String, request.Colvalues["numformat"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("timezonefull", EbDbTypes.String, request.Colvalues["timezone"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Colvalues["uid"]));
        //             resp = new UserPreferenceResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     } //adding user preference like timezone

        //     public EditUserPreferenceResponse Post(EditUserPreferenceRequest request)
        //     {
        //         EditUserPreferenceResponse resp = new EditUserPreferenceResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             string sql = "SELECT dateformat,timezone,numformat,timezoneabbre,timezonefull,locale FROM eb_users WHERE id = @id;";
        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.UserId) };

        //             var ds = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

        //             Dictionary<string, object> result = new Dictionary<string, object>();
        //             if (ds.Rows.Count > 0)
        //             {
        //                 foreach (var dr in ds.Rows)
        //                 {

        //                     result.Add("dateformat", dr[0].ToString());
        //                     result.Add("timezone", dr[1].ToString());
        //                     result.Add("numformat", dr[2].ToString());
        //                     result.Add("timezoneabbre", dr[3].ToString());
        //                     result.Add("timezonefull", dr[4].ToString());
        //                     result.Add("locale", dr[5].ToString());
        //                 }
        //             }
        //             resp.Data = result;
        //         }
        //         return resp;
        //     }

        //     public GetSubRolesResponse Any(GetSubRolesRequest request)
        //     {
        //         GetSubRolesResponse resp = new GetSubRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = string.Empty;
        //             if (request.id > 0)
        //                 sql = @"
        //                                SELECT id,role_name FROM eb_roles WHERE id != @id AND applicationid= @applicationid;
        //                                SELECT role2_id FROM eb_role2role WHERE role1_id = @id AND eb_del = 'F'";
        //             else
        //                 sql = "SELECT id,role_name FROM eb_roles WHERE applicationid= @applicationid";

        //             DbParameter[] parameters = {
        //                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id),
        //                     this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32,request.Colvalues["applicationid"])};

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> subroles = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     subroles.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("roles", subroles);
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetRolesResponse Any(GetRolesRequest request)
        //     {
        //         GetRolesResponse resp = new GetRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT id,role_name FROM eb_roles";
        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetPermissionsResponse Any(GetPermissionsRequest request)
        //     {
        //         GetPermissionsResponse resp = new GetPermissionsResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @"
        //             SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
        //             SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = 'F';
        //             SELECT applicationname FROM eb_applications WHERE id IN(SELECT applicationid FROM eb_roles WHERE id = @id);";



        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };

        //             var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
        //             List<string> _lstPermissions = new List<string>();

        //             foreach (var dr in ds.Tables[1].Rows)
        //                 _lstPermissions.Add(dr[0].ToString());

        //             resp.Permissions = _lstPermissions;
        //             Dictionary<string, object> result = new Dictionary<string, object>();
        //             foreach (var dr in ds.Tables[0].Rows)
        //             {

        //                 result.Add("rolename", dr[0].ToString());
        //                 result.Add("applicationid", Convert.ToInt32(dr[1]));
        //                 result.Add("description", dr[2].ToString());
        //             }
        //             foreach (var dr in ds.Tables[2].Rows)
        //                 result.Add("applicationname", dr[0].ToString());

        //             resp.Data = result;
        //         }
        //         return resp;
        //     } // for getting saved permissions

        //     public GetUsersResponse Any(GetUsersRequest request)
        //     {
        //         GetUsersResponse resp = new GetUsersResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT id,firstname FROM eb_users WHERE firstname ~* @searchtext";

        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     } //for user search

        //     public GetUsersRoleResponse Any(GetUsersRoleRequest request)
        //     {
        //         GetUsersRoleResponse resp = new GetUsersRoleResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @"SELECT id,firstname FROM eb_users WHERE id IN(SELECT user_id FROM eb_role2user WHERE role_id = @roleid AND eb_del = 'F')";


        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("roleid", EbDbTypes.Int32, request.id) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();

        //             foreach (EbDataRow dr in dt.Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetUser2UserGroupResponse Any(GetUser2UserGroupRequest request)
        //     {
        //         GetUser2UserGroupResponse resp = new GetUser2UserGroupResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql;
        //             if (request.id > 0)
        //             {
        //                 sql = @"SELECT id, name FROM eb_usergroup;
        //                         SELECT id,name FROM eb_usergroup WHERE id IN(SELECT groupid FROM eb_user2usergroup WHERE userid = @userid AND eb_del = 'F')";
        //             }
        //             else
        //             {
        //                 sql = "SELECT id, name FROM eb_usergroup";
        //             }

        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.id) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> usergroups = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     usergroups.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("usergroups", usergroups);
        //             }
        //             resp.Data = returndata;

        //         }
        //         return resp;
        //     }

        //     public GetUserGroupResponse Any(GetUserGroupRequest request)
        //     {
        //         GetUserGroupResponse resp = new GetUserGroupResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "";
        //             if (request.id > 0)
        //             {
        //                 sql = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
        //                        SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')";


        //                 DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };

        //                 var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
        //                 Dictionary<string, object> result = new Dictionary<string, object>();
        //                 foreach (var dr in ds.Tables[0].Rows)
        //                 {

        //                     result.Add("name", dr[1].ToString());
        //                     result.Add("description", dr[2].ToString());
        //                 }
        //                 List<int> users = new List<int>();
        //                 if (ds.Tables.Count > 1)
        //                 {
        //                     foreach (EbDataRow dr in ds.Tables[1].Rows)
        //                     {
        //                         users.Add(Convert.ToInt32(dr[0]));
        //                         result.Add(dr[0].ToString(), dr[1]);
        //                     }
        //                     result.Add("userslist", users);
        //                 }
        //                 resp.Data = result;
        //             }
        //             else
        //             {
        //                 sql = "SELECT id,name FROM eb_usergroup";
        //                 var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

        //                 Dictionary<string, object> returndata = new Dictionary<string, object>();
        //                 foreach (EbDataRow dr in dt.Tables[0].Rows)
        //                 {
        //                     returndata[dr[0].ToString()] = dr[1].ToString();
        //                 }
        //                 resp.Data = returndata;
        //             }

        //         }
        //         return resp;
        //     }

        //     public GetApplicationObjectsResponse Any(GetApplicationObjectsRequest request)
        //     {
        //         GetApplicationObjectsResponse resp = new GetApplicationObjectsResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @" SELECT 
        //                                 EO.id,EO.obj_name
        //                             FROM 
        //                                 eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
        //                             WHERE
        //                                 EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 
        //                             AND 
        //                                 EOS.id = (SELECT EOS.id FROM eb_objects_status EOS, eb_objects_ver EOV
        //                             WHERE 
        //                                 EOS.eb_obj_ver_id = EOV.id AND EO.id = EOV.eb_objects_id ORDER BY EOS.id DESC LIMIT 1) 
        //                             AND EO.applicationid = @applicationid AND EO.obj_type = @obj_type";
        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Id),
        //                 this.EbConnectionFactory.ObjectsDB.GetNewParameter("obj_type", EbDbTypes.Int32, request.objtype) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }
    }
}
