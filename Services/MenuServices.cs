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

namespace ExpressBase.ServiceStack.Services
{
    public class MenuServices : EbBaseService
    {
        public MenuServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

        public SidebarUserResponse Get(SidebarUserRequest request)
        {
            EbDataSet ds = new EbDataSet();
            Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();
            List<ObjWrap> _fav = new List<ObjWrap>();
            List<int> _favids = new List<int>();

            DbParameter[] parameters = {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("user_id",EbDbTypes.Int32,request.UserId)
            };

            if (request.SysRole.Contains("SolutionOwner"))
                ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARCHECK, string.Empty), parameters);
            else
                ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace(":Ids", string.IsNullOrEmpty(request.Ids) ? "0" : request.Ids), parameters);

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
                int appid = Convert.ToInt32(dr[5]);

                if (!_Coll.Keys.Contains<int>(appid))
                    _Coll.Add(appid, new AppWrap { Types = new Dictionary<int, TypeWrap>() });

                Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
                int typeId = Convert.ToInt32(dr[1]);
                EbObjectType ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);

                if (___otyp.IsUserFacing)
                {
                    if (!_Coll[appid].Types.Keys.Contains<int>(typeId))
                        _Coll[appid].Types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

                    ObjWrap owrap = new ObjWrap
                    {
                        Id = Convert.ToInt32(dr[0]),
                        EbObjectType = Convert.ToInt32(dr[1]),
                        Refid = dr[4].ToString(),
                        AppId = Convert.ToInt32(dr[5]),
                        EbType = ___otyp.Name,
                        DisplayName = dr[9].ToString()
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
            var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARDEV_REQUEST);

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
            string sql = @"INSERT INTO 
                                eb_objects_favourites(userid,object_id)
                            VALUES(:userid,:objectid)";
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
            return resp;
        }
    }
}
