using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using ExpressBase.Security;
using ExpressBase.Common.Constants;

namespace ExpressBase.ServiceStack.Services
{
    public class MenuServices : EbBaseService
    {
        public MenuServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public User UserObject { set; get; }

        private List<string> GetAccessIds(int lid)
        {
            List<string> ObjIds = new List<string>();
            foreach (string perm in this.UserObject.Permissions)
            {
                int id = Convert.ToInt32(perm.Split(CharConstants.DASH)[2]);
                int locid = Convert.ToInt32(perm.Split(CharConstants.COLON)[1]);
                if ((lid == locid || locid == -1) && !ObjIds.Contains(id.ToString()))
                    ObjIds.Add(id.ToString());
            }
            return ObjIds;
        }

        public SidebarUserResponse Get(SidebarUserRequest request)
        {
            EbDataSet ds = new EbDataSet();
            Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();
            List<ObjWrap> _fav = new List<ObjWrap>();
            List<int> _favids = new List<int>();

            this.UserObject = this.Redis.Get<User>(request.UserAuthId);

            DbParameter[] parameters = {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("user_id",EbDbTypes.Int32,request.UserId)
            };

            if (this.UserObject.Roles.Contains("SolutionOwner") || this.UserObject.Roles.Contains("SolutionAdmin"))
            {
                if (EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.MYSQL)
                {
                    ds = this.EbConnectionFactory.ObjectsDB.DoQueries((this.EbConnectionFactory.ObjectsDB as MySqlDB).EB_SIDEBARUSER_REQUEST_SOL_OWNER, parameters);
                }
                else
                {
                    ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARCHECK, string.Empty), parameters);
                }
            }
            else
            {
                string Ids = String.Join(",", this.GetAccessIds(request.LocationId));

                ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace(":Ids", string.IsNullOrEmpty(Ids) ? "0" : Ids), parameters);
            }

            foreach (EbDataRow row in ds.Tables[2].Rows)
            {
                _favids.Add(Convert.ToInt32(row[0]));
            }

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                var id = Convert.ToInt32(dr[0]);
                if (!appColl.Keys.Contains<int>(id))
                    appColl.Add(id, new AppObject { AppName = dr[1].ToString(), AppIcon = dr[2].ToString() });
            }

            Dictionary<int, AppWrap> _Coll = new Dictionary<int, AppWrap>();
            foreach (EbDataRow dr in ds.Tables[1].Rows)
            {
                int appid = Convert.ToInt32(dr["app_id"]);

                if (!_Coll.Keys.Contains<int>(appid))
                    _Coll.Add(appid, new AppWrap { Types = new Dictionary<int, TypeWrap>() });

                Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
                int typeId = Convert.ToInt32(dr["obj_type"]);
                EbObjectType ___otyp = (EbObjectType)Convert.ToInt32(typeId);

                if (___otyp.IsUserFacing)
                {
                    if (!_Coll[appid].Types.Keys.Contains<int>(typeId))
                        _Coll[appid].Types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

                    ObjWrap owrap = new ObjWrap
                    {
                        Id = Convert.ToInt32(dr["objectid"]),
                        EbObjectType = typeId,
                        Refid = dr["refid"].ToString(),
                        AppId = appid,
                        EbType = ___otyp.Name,
                        DisplayName = dr["display_name"].ToString()
                    };

                    _Coll[appid].Types[typeId].Objects.Add(owrap);

                    if (_favids.Contains(owrap.Id))
                    {
                        owrap.Favourite = true;
                        _fav.Add(owrap);
                    }
                }
            }
            return new SidebarUserResponse { Data = _Coll, AppList = appColl, Favourites = _fav };
        }

        public SidebarDevResponse Get(SidebarDevRequest request)
        {
            string sql = @"SELECT id, applicationname,app_icon FROM eb_applications
                                WHERE COALESCE(eb_del, 'F') = 'F' ORDER BY applicationname;
                        SELECT 
                                EO.id, EO.obj_type, EO.obj_name, EO.obj_desc, COALESCE(EO2A.app_id, 0),display_name
                            FROM 
	                            eb_objects EO
                            LEFT JOIN
	                            eb_objects2application EO2A 
                            ON
	                            EO.id = EO2A.obj_id 
                            WHERE
	                           COALESCE(EO2A.eb_del, 'F') = 'F' 
                               AND COALESCE( EO.eb_del, 'F') = 'F'
                            ORDER BY 
	                            EO.obj_type;";
            var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

            Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                var id = Convert.ToInt32(dr[0]);
                if (!appColl.Keys.Contains<int>(id))
                    appColl.Add(id, new AppObject { AppName = dr[1].ToString(), AppIcon = dr[2].ToString() });
            }

            Dictionary<int, AppWrap> _Coll = new Dictionary<int, AppWrap>();
            try
            {
                foreach (EbDataRow dr in ds.Tables[1].Rows)
                {
                    var appid = Convert.ToInt32(dr[4]);

                    if (!_Coll.Keys.Contains<int>(appid))
                        _Coll.Add(appid, new AppWrap { Types = new Dictionary<int, TypeWrap>() });

                    Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
                    var typeId = Convert.ToInt32(dr[1]);

                    if (!_Coll[appid].Types.Keys.Contains<int>(typeId))
                        _Coll[appid].Types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

                    var ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);
                    _Coll[appid].Types[typeId].Objects.Add(new ObjWrap
                    {
                        Id = (dr[0] != null) ? Convert.ToInt32(dr[0]) : 0,
                        EbObjectType = (dr[1] != null) ? Convert.ToInt32(dr[1]) : 0,
                        EbType = ___otyp.ToString(),
                        AppId = (Convert.ToInt32(dr[4]) == 0) ? 0 : Convert.ToInt32(dr[4]),
                        DisplayName = dr[5].ToString(),
                    });
                }

            }
            catch (Exception ee)
            {
                Console.BackgroundColor = ConsoleColor.Red;
                Console.WriteLine("Exception:" + ee.Message);
                Console.BackgroundColor = ConsoleColor.White;
            }

            return new SidebarDevResponse { Data = _Coll, AppList = appColl };
        }


        public AddFavouriteResponse Post(AddFavouriteRequest request)
        {
            AddFavouriteResponse resp = new AddFavouriteResponse();
            try
            {
                string sql =EbConnectionFactory.ObjectsDB.EB_ADD_FAVOURITE;
                DbParameter[] parameter =
                {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid",EbDbTypes.Int32,request.UserId),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("objectid",EbDbTypes.Int32,request.ObjId)
            };

                int rows_affected = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameter);

                if (rows_affected > 0)
                    resp.Status = true;
                else
                    resp.Status = false;
            }
            catch(Exception e)
            {
                Console.WriteLine("Exception at Adding to Fav: " + e.Message);
                resp.Status = false;
            }
            return resp;
        }

        public RemoveFavouriteResponse Post(RemoveFavouriteRequest request)
        {
            RemoveFavouriteResponse resp = new RemoveFavouriteResponse();
            try
            {
                string sql = EbConnectionFactory.ObjectsDB.EB_REMOVE_FAVOURITE;

                DbParameter[] parameter = {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid",EbDbTypes.Int32,request.UserId),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("objectid",EbDbTypes.Int32,request.ObjId)
                };

                int rows_affected = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameter);

                if (rows_affected > 0)
                    resp.Status = true;
                else
                    resp.Status = false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception at Remove From Fav: " + e.Message);
                resp.Status = false;
            }
            return resp;
        }
    }
}
