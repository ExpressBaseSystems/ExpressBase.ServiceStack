using ExpressBase.Common;
using ExpressBase.Common.Data;
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
        public MenuServices(ITenantDbFactory _dbf) : base(_dbf) { }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();
        public object Get(SidebarUserRequest request)
        {
            var Query1 = @"
SELECT
    EO.id, EO.obj_name,
    EOV.version_num, EOV.refid, APP.applicationname, APP.id as appid
FROM
    eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS ,eb_applications APP
WHERE
    EO.id = EOV.eb_objects_id 
AND 
    EOS.eb_obj_ver_id = EOV.id 
AND 
    EO.id = ANY('@Ids')  
AND 
    EOS.status = 3 
AND 
    APP.id = EO.applicationid";

            //parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@Ids", System.Data.DbType.String, request.Ids));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query1.Replace("@Ids", request.Ids));

            Dictionary<string, AppWrap> _Coll = new Dictionary<string, AppWrap>();
            foreach (EbDataRow dr in dt.Rows)
            {
                string appName = dr[4].ToString();
                if (!_Coll.Keys.Contains<string>(appName))
                    _Coll.Add(appName, new AppWrap { AppName = appName, Objects = new List<ObjWrap>() });

                _Coll[appName].Objects.Add(new ObjWrap
                {
                    Id = Convert.ToInt32(dr[0]),
                    ObjName = dr[1].ToString(),
                    VersionNumber = dr[2].ToString(),
                    Refid = dr[3].ToString(),
                    AppId = Convert.ToInt32(dr[5])
                });
            }

            return new SidebarUserResponse { Data = _Coll };
        }
    }
}
