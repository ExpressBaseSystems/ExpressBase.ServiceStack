using ExpressBase.Common;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class TenantUserServices : EbBaseService
    {
        public TenantUserServices(ITenantDbFactory _dbf, IInfraDbFactory _idbf) : base(_dbf, _idbf) { }

        private string GeneratePassword()
        {
            string strPwdchar = "abcdefghijklmnopqrstuvwxyz0123456789#+@&$ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            string strPwd = "";
            Random rnd = new Random();
            for (int i = 0; i <= 7; i++)
            {
                int iRandom = rnd.Next(0, strPwdchar.Length - 1);
                strPwd += strPwdchar.Substring(iRandom, 1);
            }
            return strPwd;
        }

        public CreateUserResponse Post(CreateUserRequest request)
        {
            CreateUserResponse resp;
            string sql = "";
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                if (request.Id > 0)
                {
                    sql = @"UPDATE eb_users SET firstname= @firstname,email= @email WHERE id = @id RETURNING id;
                            
                            INSERT INTO eb_role2user(role_id,user_id,createdby,createdat) SELECT rid,@id,@userid,NOW() FROM UNNEST(array(SELECT unnest(@roles) except 
                                        SELECT UNNEST(array(SELECT role_id from eb_role2user WHERE user_id = @id AND eb_del = FALSE)))) as rid;
                            UPDATE eb_role2user SET eb_del = true,revokedby = @userid,revokedat =NOW() WHERE role_id IN(
                                        SELECT UNNEST(array(SELECT role_id from eb_role2user WHERE user_id = @id AND eb_del = FALSE)) except SELECT UNNEST(@roles));
                           
                            INSERT INTO eb_user2usergroup(userid,groupid,createdby,createdat) SELECT gid,@id,@userid,NOW() FROM UNNEST(array(SELECT unnest(@group) except 
                                        SELECT UNNEST(array(SELECT groupid from eb_user2usergroup WHERE userid = @id AND eb_del = FALSE)))) as rid;
                            UPDATE eb_user2usergroup SET eb_del = true,revokedby = @userid,revokedat =NOW() WHERE groupid IN(
                                        SELECT UNNEST(array(SELECT groupid from eb_user2usergroup WHERE user_id = @id AND eb_del = FALSE)) except SELECT UNNEST(@group)); ";
                }
                else
                {
                    sql = @"INSERT INTO eb_users (firstname,email,pwd) VALUES (@firstname,@email,@pwd) RETURNING id,pwd;
                INSERT INTO eb_role2user (role_id,user_id,createdby,createdat) SELECT id, (CURRVAL('eb_users_id_seq')),@userid,NOW() FROM UNNEST(@roles) AS id;
                 INSERT INTO eb_user2usergroup(userid,groupid,createdby,createdat) SELECT (CURRVAL('eb_users_id_seq')), gid,@userid,NOW() FROM UNNEST(@group) AS gid";
                }
                int[] emptyarr = new int[] { };
                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("firstname", System.Data.DbType.String, request.Colvalues["firstname"]),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("email", System.Data.DbType.String, request.Colvalues["email"]),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("roles", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer,(request.Colvalues["roles"].ToString() != string.Empty? request.Colvalues["roles"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray():emptyarr)),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("group", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer,(request.Colvalues["group"].ToString() != string.Empty? request.Colvalues["group"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray():emptyarr)),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("pwd", System.Data.DbType.String,string.IsNullOrEmpty(request.Colvalues["pwd"].ToString())? GeneratePassword() :request.Colvalues["pwd"] ),
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("userid", System.Data.DbType.Int32, request.UserId),
                             this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.Id)};

                EbDataSet dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()))
                {
                    using (var service = base.ResolveService<EmailServices>())
                    {
                        service.Any(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
                    }
                }
                resp = new CreateUserResponse
                {
                    id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

                };
            } 
            return resp;
        }

        public GetUserEditResponse Any(GetUserEditRequest request)
        {
            GetUserEditResponse resp = new GetUserEditResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "SELECT firstname,email FROM eb_users WHERE id = @id;";
                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id) };

                var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                Dictionary<string, object> result = new Dictionary<string, object>();
                foreach (var dr in ds.Tables[0].Rows)
                {

                    result.Add("name", dr[0].ToString());
                    result.Add("email", dr[1].ToString());
                }
                resp.Data = result;
            }
            return resp;
        }

        public GetUserRolesResponse Any(GetUserRolesRequest request)
        {
            GetUserRolesResponse resp = new GetUserRolesResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = string.Empty;
                if (request.id > 0)
                    sql = @"SELECT id, role_name FROM eb_roles;
                             SELECT id, role_name FROM eb_roles WHERE id IN(SELECT role_id FROM eb_role2user WHERE user_id = @id ) AND eb_del = FALSE";
                else
                    sql = "SELECT id,role_name FROM eb_roles";

                DbParameter[] parameters = {
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id)};

                var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                Dictionary<string, object> returndata = new Dictionary<string, object>();
                List<int> subroles = new List<int>();
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }

                if (dt.Tables.Count > 1)
                {
                    foreach (EbDataRow dr in dt.Tables[1].Rows)
                    {
                        subroles.Add(Convert.ToInt32(dr[0]));
                    }
                    returndata.Add("roles", subroles);
                }
                resp.Data = returndata;
            }
            return resp;
        }


        public RBACRolesResponse Post(RBACRolesRequest request)
        {
            RBACRolesResponse resp;
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @users, @dependants,@permission );";
                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                int[] emptyarr = new int[] { };
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_id", System.Data.DbType.Int32, request.Colvalues["roleid"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["Description"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("role_name", System.Data.DbType.String, request.Colvalues["role_name"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationid", System.Data.DbType.Int32, request.Colvalues["applicationid"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("createdby", System.Data.DbType.Int32, request.UserId));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("permission", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => n.ToString()).ToArray() : new string[] { }));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("users", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("dependants", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"].ToString().Replace("[", "").Replace("]", "").Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));

                resp = new RBACRolesResponse
                {
                    id = Convert.ToInt32(cmd.ExecuteScalar())

                };
            }
            return resp;
        }

        public CreateUserGroupResponse Post(CreateUserGroupRequest request)
        {
            CreateUserGroupResponse resp;
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "";
                if (request.Id > 0)
                {
                    sql = @"UPDATE eb_usergroup SET name = @name,description = @description WHERE id = @id;
                                    INSERT INTO eb_user2usergroup(userid,groupid) SELECT uid,@id FROM UNNEST(array(SELECT unnest(@users) except 
                                        SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = FALSE)))) as uid;
                                    UPDATE eb_user2usergroup SET eb_del = true WHERE userid IN(
                                        SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = FALSE)) except SELECT UNNEST(@users));";
                }
                else
                {
                    sql = @"INSERT INTO eb_usergroup (name,description) VALUES (@name,@description) RETURNING id;
                                       INSERT INTO eb_user2usergroup (userid,groupid) SELECT id, (CURRVAL('eb_usergroup_id_seq')) FROM UNNEST(@users) AS id";
                }

                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                int[] emptyarr = new int[] { };
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("name", System.Data.DbType.String, request.Colvalues["groupname"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("description", System.Data.DbType.String, request.Colvalues["description"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("users", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, (request.Colvalues["userlist"].ToString() != string.Empty) ? request.Colvalues["userlist"].ToString().Split(',').Select(n => Convert.ToInt32(n)).ToArray() : emptyarr));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.Id));
                resp = new CreateUserGroupResponse
                {
                    id = Convert.ToInt32(cmd.ExecuteScalar())

                };
            }
            return resp;
        } //user group creation

        public UserPreferenceResponse Post(UserPreferenceRequest request)
        {
            UserPreferenceResponse resp;
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                var cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_users SET locale=@locale,timezone=@timezone,dateformat=@dateformat,numformat=@numformat,timezonefull=@timezonefull WHERE id=@id");
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("locale", System.Data.DbType.String, request.Colvalues["locale"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("timezone", System.Data.DbType.String, request.Colvalues["timecode"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("dateformat", System.Data.DbType.String, request.Colvalues["dateformat"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("numformat", System.Data.DbType.String, request.Colvalues["numformat"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("timezonefull", System.Data.DbType.String, request.Colvalues["timezone"]));
                cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int64, request.Colvalues["uid"]));
                resp = new UserPreferenceResponse
                {
                    id = Convert.ToInt32(cmd.ExecuteScalar())

                };
            }
            return resp;
        } //adding user preference like timezone

        public GetSubRolesResponse Any(GetSubRolesRequest request)
        {
            GetSubRolesResponse resp = new GetSubRolesResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = string.Empty;
                if (request.id > 0)
                    sql = @"
                                   SELECT id,role_name FROM eb_roles WHERE id != @id AND applicationid= @applicationid;
                                   SELECT role2_id FROM eb_role2role WHERE role1_id = @id AND eb_del = FALSE";
                else
                    sql = "SELECT id,role_name FROM eb_roles WHERE applicationid= @applicationid";

                DbParameter[] parameters = {
                            this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id),
                        this.TenantDbFactory.ObjectsDB.GetNewParameter("applicationid", System.Data.DbType.Int32,request.Colvalues["applicationid"])};

                var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                Dictionary<string, object> returndata = new Dictionary<string, object>();
                List<int> subroles = new List<int>();
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }

                if (dt.Tables.Count > 1)
                {
                    foreach (EbDataRow dr in dt.Tables[1].Rows)
                    {
                        subroles.Add(Convert.ToInt32(dr[0]));
                    }
                    returndata.Add("roles", subroles);
                }
                resp.Data = returndata;
            }
            return resp;
        }

        public GetRolesResponse Any(GetRolesRequest request)
        {
            GetRolesResponse resp = new GetRolesResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "SELECT id,role_name FROM eb_roles";
                var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql);

                Dictionary<string, object> returndata = new Dictionary<string, object>();
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }
                resp.Data = returndata;
            }
            return resp;
        }

        public GetPermissionsResponse Any(GetPermissionsRequest request)
        {
            GetPermissionsResponse resp = new GetPermissionsResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"
                SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
                SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = FALSE;
                SELECT obj_name FROM eb_objects WHERE id IN(SELECT applicationid FROM eb_roles WHERE id = @id);
                SELECT refid FROM eb_objects_ver WHERE eb_objects_id IN(SELECT applicationid FROM eb_roles WHERE id = @id)";



                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

                var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);
                List<string> _lstPermissions = new List<string>();

                foreach (var dr in ds.Tables[1].Rows)
                    _lstPermissions.Add(dr[0].ToString());

                resp.Permissions = _lstPermissions;
                Dictionary<string, object> result = new Dictionary<string, object>();
                foreach (var dr in ds.Tables[0].Rows)
                {

                    result.Add("rolename", dr[0].ToString());
                    result.Add("applicationid", Convert.ToInt32(dr[1]));
                    result.Add("description", dr[2].ToString());
                }


                foreach (var dr in ds.Tables[2].Rows)
                    result.Add("applicationname", dr[0].ToString());

                foreach (var dr in ds.Tables[3].Rows)
                    result.Add("dominantrefid", dr[0].ToString());

                resp.Data = result;
            }
            return resp;
        } // for getting saved permissions

        public GetUsersResponse Any(GetUsersRequest request)
        {
            GetUsersResponse resp = new GetUsersResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "SELECT id,firstname FROM eb_users WHERE firstname ~* @searchtext";

                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("searchtext", System.Data.DbType.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

                var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                Dictionary<string, object> returndata = new Dictionary<string, object>();
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }
                resp.Data = returndata;
            }
            return resp;
        } //for user search

        public GetUsersRoleResponse Any(GetUsersRoleRequest request)
        {
            GetUsersRoleResponse resp = new GetUsersRoleResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"SELECT id,firstname FROM eb_users WHERE id IN(SELECT user_id FROM eb_role2user WHERE role_id = @roleid AND eb_del = FALSE)";


                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("roleid", System.Data.DbType.Int32, request.id) };

                var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

                Dictionary<string, object> returndata = new Dictionary<string, object>();

                foreach (EbDataRow dr in dt.Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }
                resp.Data = returndata;
            }
            return resp;
        }

        public GetUser2UserGroupResponse Any(GetUser2UserGroupRequest request)
        {
            GetUser2UserGroupResponse resp = new GetUser2UserGroupResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql;
                if (request.id > 0)
                {
                    sql = @"SELECT id, name FROM eb_usergroup;
                            SELECT id,name FROM eb_usergroup WHERE id IN(SELECT groupid FROM eb_user2usergroup WHERE userid = @userid AND eb_del = FALSE)";
                }
                else
                {
                    sql = "SELECT id, name FROM eb_usergroup";
                }

                DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("userid", System.Data.DbType.Int32, request.id) };

                var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);

                Dictionary<string, object> returndata = new Dictionary<string, object>();
                List<int> usergroups = new List<int>();
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    returndata[dr[0].ToString()] = dr[1].ToString();
                }

                if (dt.Tables.Count > 1)
                {
                    foreach (EbDataRow dr in dt.Tables[1].Rows)
                    {
                        usergroups.Add(Convert.ToInt32(dr[0]));
                    }
                    returndata.Add("usergroups", usergroups);
                }
                resp.Data = returndata;

            }
            return resp;
        }

        public GetUserGroupResponse Any(GetUserGroupRequest request)
        {
            GetUserGroupResponse resp = new GetUserGroupResponse();
            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = "";
                if (request.id > 0)
                {
                    sql = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
                           SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = FALSE)";


                    DbParameter[] parameters = { this.TenantDbFactory.ObjectsDB.GetNewParameter("id", System.Data.DbType.Int32, request.id) };

                    var ds = this.TenantDbFactory.ObjectsDB.DoQueries(sql, parameters);
                    Dictionary<string, object> result = new Dictionary<string, object>();
                    foreach (var dr in ds.Tables[0].Rows)
                    {

                        result.Add("name", dr[1].ToString());
                        result.Add("description", dr[2].ToString());
                    }
                    List<int> users = new List<int>();
                    if (ds.Tables.Count > 1)
                    {
                        foreach (EbDataRow dr in ds.Tables[1].Rows)
                        {
                            users.Add(Convert.ToInt32(dr[0]));
                            result.Add(dr[0].ToString(), dr[1]);
                        }
                        result.Add("userslist", users);
                    }
                    resp.Data = result;
                }
                else
                {
                    sql = "SELECT id,name FROM eb_usergroup";
                    var dt = this.TenantDbFactory.ObjectsDB.DoQueries(sql);

                    Dictionary<string, object> returndata = new Dictionary<string, object>();
                    foreach (EbDataRow dr in dt.Tables[0].Rows)
                    {
                        returndata[dr[0].ToString()] = dr[1].ToString();
                    }
                    resp.Data = returndata;
                }

            }
            return resp;
        }
    }
}
