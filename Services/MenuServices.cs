using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class MenuServices : EbBaseService
    {
        public MenuServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();
        public object Get(SidebarUserRequest request)
        {
            //            var Query1 = @"
            //SELECT id, applicationname
            //FROM eb_applications;
            //SELECT
            //    EO.id, EO.obj_type, EO.obj_name,
            //    EOV.version_num, EOV.refid, EO2A.app_id,EO.obj_desc
            //FROM
            //    eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EO2A 
            //WHERE
            //    EO.id = EOV.eb_objects_id 
            //AND 
            //    EOS.eb_obj_ver_id = EOV.id 
            //AND 
            //    EO.id = ANY('@Ids')  
            //AND 
            //    EOS.status = 3 
            //AND EO.id = EO2A.obj_id 
            //AND EO2A.eb_del = 'F';";

            //parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@Ids", System.Data.DbType.String, request.Ids));
            var ds = new EbDataSet();
            if(request.SysRole.Contains("SolutionOwner"))
                ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace("AND EO.id = ANY(:Ids) ", string.Empty));
            else
                ds = this.EbConnectionFactory.ObjectsDB.DoQueries(this.EbConnectionFactory.ObjectsDB.EB_SIDEBARUSER_REQUEST.Replace(":Ids", string.IsNullOrEmpty(request.Ids) ? "0" : request.Ids));

            Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();
            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                var id = Convert.ToInt32(dr[0]);
                if (!appColl.Keys.Contains<int>(id))
                    appColl.Add(id, new AppObject { AppName = dr[1].ToString() });
            }

            Dictionary<int, AppWrap> _Coll = new Dictionary<int, AppWrap>();

            foreach (EbDataRow dr in ds.Tables[1].Rows)
            {
                var appid = Convert.ToInt32(dr[5]);

                if (!_Coll.Keys.Contains<int>(appid))
                    _Coll.Add(appid, new AppWrap {  Types =new Dictionary<int, TypeWrap>() });

                Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
                var typeId = Convert.ToInt32(dr[1]);

                if (!_Coll[appid].Types.Keys.Contains<int>(typeId))
                    _Coll[appid].Types.Add(typeId, new TypeWrap {Objects = new List<ObjWrap>() });

				var ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);

				_Coll[appid].Types[typeId].Objects.Add(new ObjWrap
                {
                    Id = Convert.ToInt32(dr[0]),
                    EbObjectType = Convert.ToInt32(dr[1]),
                    ObjName = dr[2].ToString(),
                    VersionNumber = dr[3].ToString(),
                    Refid = dr[4].ToString(),
                    AppId = Convert.ToInt32(dr[5]),
                    Description = dr[6].ToString(),
                    EbType = ___otyp.Name

                });
            }

            return new SidebarUserResponse { Data = _Coll, AppList = appColl };
        }

   //     public object Get(SidebarDevRequest request)
   //     {
   //         var Query1 = @"
   //         SELECT id, applicationname FROM eb_applications;
   //         SELECT EO.id, EO.obj_type, EO.obj_name,EO.obj_desc,EO.applicationid FROM
   //         eb_objects EO ORDER BY EO.obj_type;";

   //         //parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@Ids", System.Data.DbType.String, request.Ids));
   //         var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(Query1);

   //         Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();

   //         foreach (EbDataRow dr in ds.Tables[0].Rows)
   //         {
   //             var id = Convert.ToInt32(dr[0]);
   //             if (!appColl.Keys.Contains<int>(id))
   //                 appColl.Add(id, new AppObject { AppName = dr[1].ToString() });
   //         }

   //         Dictionary<int, AppWrap> _Coll = new Dictionary<int, AppWrap>();

   //         try
			//{
			//	foreach (EbDataRow dr in ds.Tables[1].Rows)
			//	{
   //                 var appid = Convert.ToInt32(dr[4]);

   //                 if (!_Coll.Keys.Contains<int>(appid))
   //                     _Coll.Add(appid, new AppWrap { Types = new Dictionary<int, TypeWrap>() });

   //                 Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
   //                 var typeId = Convert.ToInt32(dr[1]);

   //                 if (!_Coll[appid].Types.Keys.Contains<int>(typeId))
   //                     _Coll[appid].Types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

   //                 var ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);

   //                 _Coll[appid].Types[typeId].Objects.Add(new ObjWrap
   //                 {
   //                     Id = (dr[0] != null) ? Convert.ToInt32(dr[0]) : 0,
   //                     EbObjectType = (dr[1] != null) ? Convert.ToInt32(dr[1]) : 0,
   //                     ObjName = dr[2].ToString(),
   //                     Description = dr[3].ToString(),
   //                     EbType = ___otyp.ToString(),
   //                     AppId = (Convert.ToInt32(dr[4]) == 0) ? 0 : Convert.ToInt32(dr[4])

   //                 });

			//	}
			//}catch (Exception ee)
			//{

			//}

   //         return new SidebarDevResponse{ Data = _Coll, AppList = appColl };
   //     }

        public object Get(SidebarDevRequest request)
        {
            var Query1 = @"SELECT id, applicationname FROM eb_applications;
                            SELECT 
                                EO.id, EO.obj_type, EO.obj_name, EO.obj_desc,EO2A.app_id 
                            FROM 
                                eb_objects EO
                            INNER JOIN
                                eb_objects2application EO2A 
                            ON
                                EO.id = EO2A.obj_id 
                            WHERE
                                EO2A.eb_del = 'F' 
                            ORDER BY 
                                EO.obj_type;";

            //parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@Ids", System.Data.DbType.String, request.Ids));
            var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(Query1);

            Dictionary<int, AppObject> appColl = new Dictionary<int, AppObject>();

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                var id = Convert.ToInt32(dr[0]);
                if (!appColl.Keys.Contains<int>(id))
                    appColl.Add(id, new AppObject { AppName = dr[1].ToString() });
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
                        ObjName = dr[2].ToString(),
                        Description = dr[3].ToString(),
                        EbType = ___otyp.ToString(),
                        AppId = (Convert.ToInt32(dr[4]) == 0) ? 0 : Convert.ToInt32(dr[4])

                    });

                }
               
            }
            catch (Exception ee)
            {
                Console.WriteLine("Exception:" + ee.Message);
            }

            return new SidebarDevResponse { Data = _Coll, AppList = appColl };
        }
    }
}
