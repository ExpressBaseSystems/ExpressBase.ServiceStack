using ExpressBase.Common;

using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Syncfusion.XlsIO;
using System.IO;
using Microsoft.AspNetCore.Mvc;
using ExpressBase.Objects.Objects;
using Newtonsoft.Json;
using ExpressBase.Common.Excel;

namespace ExpressBase.ServiceStack.Services
{
    public class ExcelServices : EbBaseService
    {
        public ExcelServices(IEbConnectionFactory _dbf) : base() { }

        public ExcelDownloadResponse Get(ExcelDownloadRequest request)
        {
            ExcelDownloadResponse response = new ExcelDownloadResponse();

            //.......Get Webform obj ........
            EbWebForm _form = this.Redis.Get<EbWebForm>(request._refid);
            if (_form == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request._refid });
                _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbWebForm>(request._refid, _form);
            }
            _form.AfterRedisGet(this);

            List< ColumnsInfo> _cols = new List<ColumnsInfo>();
            foreach(var _tbl in _form.FormSchema.Tables)
            {
                string _tblName = _tbl.TableName;
                foreach(var _col in _tbl.Columns)
                {
                    _cols.Add(new ColumnsInfo { Name = _col.Control.Name, Label = _col.Control.Label, DbType = _col.Control.EbDbType, TableName = _tblName });
                    //_cols.Add(new ColumnsInfo { Name = _col.Control.Name, DbType = _col.Control.EbDbType, TableName = _tblName });
                }
            }

            string _formName = _form.Name;
    

            return new ExcelDownloadResponse{ colsInfo = _cols, formName = _formName }; 
        }
    }
   
}
