using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.Data;
using System.Data.Common;
using ExpressBase.Common;
using ExpressBase.Security.Core;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;

namespace ExpressBase.ServiceStack.Services
{
    public class SecurityServices : EbBaseService
	{
		public SecurityServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		//------COMMON LIST-------------------------------------

		public GetUsersResponse1 Any(GetUsersRequest1 request)
		{
			GetUsersResponse1 resp = new GetUsersResponse1();
			
				string sql = "SELECT id,fullname,email,nickname,sex,phnoprimary,statusid FROM eb_users WHERE LOWER(fullname) LIKE LOWER('%' || :searchtext || '%') AND eb_del = 'F';";

				DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : "") };

				var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

				List<Eb_User_ForCommonList> returndata = new List<Eb_User_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_User_ForCommonList {
						Id = Convert.ToInt32(dr[0]),
						Name = dr[1].ToString(),
						Email = dr[2].ToString(),
						Nick_Name = dr[3].ToString(),
						Sex = dr[4].ToString(),
						Phone_Number = dr[5].ToString(),
						Status = (((EbUserStatus)Convert.ToInt32(dr[6])).ToString())
					});
				}
				resp.Data = returndata;
			
			return resp;
		} //for user search

		public GetAnonymousUserResponse Any(GetAnonymousUserRequest request)
		{
			GetAnonymousUserResponse resp = new GetAnonymousUserResponse();
			using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
			{
				con.Open();
				string sql = @"SELECT A.id, A.fullname, A.email, A.phoneno, A.socialid, A.firstvisit, A.lastvisit, A.totalvisits, B.applicationname 
								FROM eb_usersanonymous A, eb_applications B WHERE A.appid = B.id AND A.ebuserid = 1;";

				DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

				List<Eb_AnonymousUser_ForCommonList> returndata = new List<Eb_AnonymousUser_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_AnonymousUser_ForCommonList {
						Id = Convert.ToInt32(dr[0]),
						Full_Name = dr[1].ToString(),
						Email_Id = dr[2].ToString(),
						Phone_No = dr[3].ToString(),
						Social_Id = dr[4].ToString(),
						First_Visit = (dr[5].ToString() == DateTime.MinValue.ToString()) ? "": dr[5].ToString(),
						Last_Visit = (dr[6].ToString() == DateTime.MinValue.ToString()) ? "" : dr[6].ToString(),
						Total_Visits = Convert.ToInt32(dr[7]),
						App_Name = dr[8].ToString()
					});
				}
				resp.Data = returndata;
			}
			return resp;
		}

		public GetUserGroupResponse1 Any(GetUserGroupRequest1 request)
		{
			GetUserGroupResponse1 resp = new GetUserGroupResponse1();
			using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
			{
				con.Open();
				string sql = "SELECT id,name,description FROM eb_usergroup WHERE LOWER(name) LIKE LOWER('%' || :searchtext || '%') AND eb_del = 'F';";

				DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);

				List<Eb_UserGroup_ForCommonList> returndata = new List<Eb_UserGroup_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_UserGroup_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString() });
				}
				resp.Data = returndata;
			}
			return resp;
		} //for usergroup search

		public GetRolesResponse1 Any(GetRolesRequest1 request)
		{
			GetRolesResponse1 resp = new GetRolesResponse1();
			using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
			{
				con.Open();
				//string sql = @"";
				//string sql = "SELECT R.id,R.role_name,R.description,A.applicationname FROM eb_roles R, eb_applications A WHERE R.applicationid = A.id AND R.role_name ~* @searchtext";

				DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

				var dt = this.EbConnectionFactory.DataDB.DoQueries(this.EbConnectionFactory.DataDB.EB_GETROLESRESPONSE_QUERY, parameters);

				List<Eb_Roles_ForCommonList> returndata = new List<Eb_Roles_ForCommonList>();
				foreach (EbDataRow dr in dt.Tables[0].Rows)
				{
					returndata.Add(new Eb_Roles_ForCommonList { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[2].ToString(),Application_Name = dr[3].ToString(), SubRole_Count = Convert.ToInt32(dr[4]), User_Count = Convert.ToInt32(dr[5]), Permission_Count = Convert.ToInt32(dr[6]) });
				}
				resp.Data = returndata;
			}
			return resp;
		} //for roles search



		//----MANAGE USER START---------------------------------
		public GetManageUserResponse Any(GetManageUserRequest request)
		{
			GetManageUserResponse resp = new GetManageUserResponse();
			string sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
                        SELECT id, name,description FROM eb_usergroup ORDER BY name;
						SELECT id, role1_id, role2_id FROM eb_role2role WHERE eb_del = 'F';";
			if (request.Id > 1)
			{
				sql += @"SELECT fullname,nickname,email,alternateemail,dob,sex,phnoprimary,phnosecondary,landline,phextension,fbid,fbname,statusid,hide,preferencesjson
						FROM eb_users WHERE id = :id;
						SELECT role_id FROM eb_role2user WHERE user_id = :id AND eb_del = 'F';
						SELECT groupid FROM eb_user2usergroup WHERE userid = :id AND eb_del = 'F';";
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
				resp.Role2RoleList.Add(new Eb_RoleToRole() {
					Id = Convert.ToInt32(dr[0]),
					Dominant = Convert.ToInt32(dr[1]),
					Dependent = Convert.ToInt32(dr[2])
				});
			}


			if (request.Id > 1)
			{
				resp.UserData = new Dictionary<string, string>();
				foreach (var dr in ds.Tables[3].Rows)
				{
					resp.UserData.Add("id", request.Id.ToString());
					resp.UserData.Add("fullname", dr[0].ToString());
					resp.UserData.Add("nickname", dr[1].ToString());
					resp.UserData.Add("email", dr[2].ToString());
					resp.UserData.Add("alternateemail", dr[3].ToString());
					resp.UserData.Add("dob", dr[4].ToString().Substring(0, 10));
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
				}

				resp.UserRoles = new List<int>();
				foreach (var dr in ds.Tables[4].Rows)
					resp.UserRoles.Add(Convert.ToInt32(dr[0]));

				resp.UserGroups = new List<int>();
				foreach (var dr in ds.Tables[5].Rows)
					resp.UserGroups.Add(Convert.ToInt32(dr[0]));
			}

			return resp;
		}

		

		public UniqueCheckResponse Any(UniqueCheckRequest request)
		{
			string sql = string.Empty;
            DbParameter[] parameters = new DbParameter[] { };

            if (!string.IsNullOrEmpty(request.email))
            {
                sql = "SELECT id FROM eb_users WHERE LOWER(email) LIKE LOWER('%' || :email || '%') AND eb_del = 'F'";
                parameters =new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, string.IsNullOrEmpty(request.email)?"":request.email)  };
            }
				
			if (!string.IsNullOrEmpty(request.roleName))
            {
                sql = "SELECT id FROM eb_roles WHERE LOWER(role_name) LIKE LOWER(:roleName);";
                parameters = new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("roleName", EbDbTypes.String, string.IsNullOrEmpty(request.roleName) ? "" : request.roleName) };
            }		
			var dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
			if (dt.Rows.Count > 0)
			{
				return new UniqueCheckResponse { unrespose = true};
			}
			else
			{
                return new UniqueCheckResponse { unrespose = false };
            }
		}

		public ChangeUserPasswordResponse Any(ChangeUserPasswordRequest request)
		{
			string sql = "UPDATE eb_users SET pwd = :newpwd WHERE id = :userid AND pwd = :oldpwd;";
			DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("oldpwd", EbDbTypes.String, (request.OldPwd + request.Email).ToMD5Hash()),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("newpwd", EbDbTypes.String, (request.NewPwd + request.Email).ToMD5Hash())
			};
			return new ChangeUserPasswordResponse() {
				isSuccess = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameters) > 0 ? true : false
			};
		}


		public SaveUserResponse Post(SaveUserRequest request)
		{
			SaveUserResponse resp;
            string sql = this.EbConnectionFactory.DataDB.EB_SAVEUSER_QUERY;
			
			
			string password = (request.Password + request.EmailPrimary).ToMD5Hash(); 
				
				DbParameter[] parameters = 
					{
						this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
						this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
						this.EbConnectionFactory.DataDB.GetNewParameter("fullname", EbDbTypes.String, request.FullName),
						this.EbConnectionFactory.DataDB.GetNewParameter("nickname", EbDbTypes.String, request.NickName),
						this.EbConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, request.EmailPrimary),
						this.EbConnectionFactory.DataDB.GetNewParameter("pwd", EbDbTypes.String,password),
						this.EbConnectionFactory.DataDB.GetNewParameter("dob", EbDbTypes.Date, request.DateOfBirth),
						this.EbConnectionFactory.DataDB.GetNewParameter("sex", EbDbTypes.String, request.Sex),
						this.EbConnectionFactory.DataDB.GetNewParameter("alternateemail", EbDbTypes.String, request.EmailSecondary),
						this.EbConnectionFactory.DataDB.GetNewParameter("phprimary", EbDbTypes.String, request.PhonePrimary),
						this.EbConnectionFactory.DataDB.GetNewParameter("phsecondary", EbDbTypes.String, request.PhoneSecondary),
						this.EbConnectionFactory.DataDB.GetNewParameter("phlandphone", EbDbTypes.String, request.LandPhone),
						this.EbConnectionFactory.DataDB.GetNewParameter("extension", EbDbTypes.String, request.PhoneExtension),
						this.EbConnectionFactory.DataDB.GetNewParameter("fbid", EbDbTypes.String, request.FbId),
						this.EbConnectionFactory.DataDB.GetNewParameter("fbname", EbDbTypes.String, request.FbName),
						this.EbConnectionFactory.DataDB.GetNewParameter("roles", EbDbTypes.String, (request.Roles != string.Empty? request.Roles : string.Empty)),
						this.EbConnectionFactory.DataDB.GetNewParameter("groups", EbDbTypes.String, (request.UserGroups != string.Empty? request.UserGroups : string.Empty)),
						this.EbConnectionFactory.DataDB.GetNewParameter("statusid", EbDbTypes.Int32, Convert.ToInt32(request.StatusId)),
						this.EbConnectionFactory.DataDB.GetNewParameter("hide", EbDbTypes.String, request.Hide),
						this.EbConnectionFactory.DataDB.GetNewParameter("anonymoususerid", EbDbTypes.Int32, request.AnonymousUserId),
						this.EbConnectionFactory.DataDB.GetNewParameter("preference", EbDbTypes.String, request.Preference),
					};
				
				EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
				
				//if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
				//{
				//	using (var service = base.ResolveService<EmailService>())
				//	{
				//		//  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
				//	}
				//}
				resp = new SaveUserResponse
				{
					id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

				};
			
			return resp;
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
			
			DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.Id) };
			var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
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
			return new GetManageAnonymousUserResponse {UserData = Udata };
		}

		public UpdateAnonymousUserResponse Any(UpdateAnonymousUserRequest request)
		{
			string sql = @"UPDATE eb_usersanonymous 
								SET fullname=@fullname, email=@emailid, phoneno=@phoneno, remarks = @remarks, modifiedby = @modifiedby, modifiedat = NOW()
								WHERE id=@id";
			DbParameter[] parameters = {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@fullname", EbDbTypes.String, request.FullName),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@emailid", EbDbTypes.String, request.EmailID),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@phoneno", EbDbTypes.String, request.PhoneNumber),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@remarks", EbDbTypes.String, request.Remarks),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@modifiedby", EbDbTypes.Int32, request.UserId),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.Id)
			};
			return new UpdateAnonymousUserResponse {RowAffected = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameters) };
		}
		
		public ConvertAnonymousUserResponse Any(ConvertAnonymousUserRequest request)
		{
			//WORK NOT COMPLETED
			string sql = @"SELECT * FROM eb_convertanonymoususer2user(@userid, @id, @fullname, @email, @phnoprimary, @remarks);";
			DbParameter[] parameters = {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("fullname", EbDbTypes.String, request.FullName),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.EmailID),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("phnoprimary", EbDbTypes.String, request.PhoneNumber),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("remarks", EbDbTypes.String, request.Remarks)
			};
			EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
			return new ConvertAnonymousUserResponse { status = (dt.Tables.Count > 0) ? Convert.ToInt32(dt.Tables[0].Rows[0][0]): 0 };
		}

		
		//------MANAGE USER GROUP START------------------------------

		public GetManageUserGroupResponse Any(GetManageUserGroupRequest request)
		{
			List<DbParameter> parameters = new List<DbParameter>();
			List<Eb_Users> _usersList = new List<Eb_Users>();
			Dictionary<string, object> _userGroupInfo = new Dictionary<string, object>();
			if (request.id > 0)
			{
				string query = @"SELECT id,name,description FROM eb_usergroup WHERE id = :id;
							SELECT U.id,U.fullname,U.email FROM eb_users U, eb_user2usergroup G WHERE G.groupid = :id AND U.id=G.userid AND G.eb_del = 'F';";
				parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.id));
				var ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters.ToArray());
				if (ds.Tables.Count > 0)
				{
					_userGroupInfo.Add("id", Convert.ToInt32(ds.Tables[0].Rows[0][0]));
					_userGroupInfo.Add("name", ds.Tables[0].Rows[0][1].ToString());
					_userGroupInfo.Add("description", ds.Tables[0].Rows[0][2].ToString());
					foreach (EbDataRow dr in ds.Tables[1].Rows)
					{
						_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString() });
					}
				}
			}
			else
				_userGroupInfo.Add("id", 0);

			return new GetManageUserGroupResponse() { SelectedUserGroupInfo = _userGroupInfo, UsersList = _usersList };
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
			SaveUserGroupResponse resp;
			string sql = this.EbConnectionFactory.DataDB.EB_SAVEUSERGROUP_QUERY;
			using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
			{
				con.Open();
				
				int[] emptyarr = new int[] { };
				DbParameter[] parameters =
					{
						this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
						this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id),
						this.EbConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String, request.Name),
						this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Description),
						this.EbConnectionFactory.DataDB.GetNewParameter("users", EbDbTypes.String,(request.Users != string.Empty? request.Users : string.Empty))
					};

				EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);



				//if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
				//{
				//	using (var service = base.ResolveService<EmailService>())
				//	{
				//		//  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
				//	}
				//}
				resp = new SaveUserGroupResponse
				{
					id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

				};
			}
			return resp;
		}

		//----MANAGE ROLES START---------------------------------------
		public GetManageRolesResponse Any(GetManageRolesRequest request)
		{
			string query = null;
			
            //old query
            //SELECT DISTINCT EO.id, EO.obj_name, EO.obj_type, EO.applicationid
            //FROM eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS
            //WHERE EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 AND EO.applicationid > 0;
            query = this.EbConnectionFactory.DataDB.EB_GETMANAGEROLESRESPONSE_QUERY;
			 if (request.id > 0)
			{
                query += this.EbConnectionFactory.DataDB.EB_GETMANAGEROLESRESPONSE_QUERY_EXTENDED;
			}
            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };
			var ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters);
			ApplicationCollection _applicationCollection = null;
			List<Eb_RoleObject> _roleList = new List<Eb_RoleObject>();
			List<Eb_RoleToRole> _r2rList = new List<Eb_RoleToRole>();
			
			if (ds.Tables.Count > 0)
			{
				//PROCESSED RESULT
				_applicationCollection = new ApplicationCollection(ds.Tables[0], ds.Tables[1]);
				//---------------
				foreach (EbDataRow dr in ds.Tables[2].Rows)
				{
                    _roleList.Add(new Eb_RoleObject() {
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
					_r2rList.Add(new Eb_RoleToRole() {
						Id = Convert.ToInt32(dr[0]),
						Dominant = Convert.ToInt32(dr[1]),
						Dependent = Convert.ToInt32(dr[2])
					});
				}

			}
			Dictionary<string, object> RoleInfo = new Dictionary<string, object>();
			List<string> Permission = new List<string>();
			List<Eb_Users> _usersList = new List<Eb_Users>();

			if (ds.Tables.Count > 4)
			{
				RoleInfo.Add("RoleName", ds.Tables[4].Rows[0][0].ToString());
				RoleInfo.Add("AppId", Convert.ToInt32(ds.Tables[4].Rows[0][1]));
				RoleInfo.Add("RoleDescription", ds.Tables[4].Rows[0][2].ToString());
				RoleInfo.Add("IsAnonymous", (ds.Tables[4].Rows[0][3].ToString() == "T")?true:false);
				RoleInfo.Add("AppName", ds.Tables[6].Rows[0][0].ToString());
				RoleInfo.Add("AppDescription", ds.Tables[6].Rows[0][1].ToString());
				foreach (var dr in ds.Tables[5].Rows)
					Permission.Add(dr[0].ToString());
				foreach (EbDataRow dr in ds.Tables[7].Rows)
				{
					_usersList.Add(new Eb_Users() {
						Id = Convert.ToInt32(dr[0]),
						Name = dr[1].ToString(),
						Email = dr[2].ToString(),
						Role2User_Id = Convert.ToInt32(dr[3])
					});
				}
			}
			return new GetManageRolesResponse() { ApplicationCollection = _applicationCollection, SelectedRoleInfo = RoleInfo, PermissionList = Permission, RoleList = _roleList, Role2RoleList = _r2rList, UsersList = _usersList };
		}
		
		public GetUserDetailsResponse Any(GetUserDetailsRequest request)
		{
			string query = null;
			query = string.Format(@"SELECT id,fullname,email FROM eb_users WHERE LOWER(fullname) LIKE LOWER('%' || :searchtext || '%') AND eb_del = 'F' ORDER BY fullname ASC;");
            DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("searchtext", EbDbTypes.String, request.SearchText) };

            var ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters);
            List<Eb_Users> _usersList = new List<Eb_Users>();
			if (ds.Tables.Count > 0)
			{
				foreach (EbDataRow dr in ds.Tables[0].Rows)
				{
					_usersList.Add(new Eb_Users() { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Email = dr[2].ToString()});
				}
			}
			return new GetUserDetailsResponse() { UserList = _usersList };
		}
		
		public SaveRoleResponse Post(SaveRoleRequest request)
		{
			SaveRoleResponse resp;
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = this.EbConnectionFactory.DataDB.EB_SAVEROLES_QUERY;
                int[] emptyarr = new int[] { };
                DbParameter[] parameters ={ this.EbConnectionFactory.DataDB.GetNewParameter("role_id", EbDbTypes.Int32, request.Colvalues["roleid"]),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["Description"]),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("is_anonym", EbDbTypes.String, (request.Colvalues["IsAnonymous"]).Equals("true")?"T":"F"),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("role_name", EbDbTypes.String, request.Colvalues["role_name"]),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Colvalues["applicationid"]),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.UserId),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("permission", EbDbTypes.String , (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"]: string.Empty),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"] : string.Empty),
                                        this.EbConnectionFactory.DataDB.GetNewParameter("dependants", EbDbTypes.String, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"] : string.Empty) };

                var ds = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
                resp = new SaveRoleResponse
				{
					id =Convert.ToInt32(ds.Rows[0][0])
				};
			}
			return resp;
		}
		
	}	

}
