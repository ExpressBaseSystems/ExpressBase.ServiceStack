using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.IdentityModel.Tokens.Jwt;
using System.Data;
using Npgsql;
using ExpressBase.Common.Structures;
using System.Data.Common;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("ds")]
    [Authenticate]
    public class DataSourceService : EbBaseService
    {
        public DataSourceService(IEbConnectionFactory _dbf) : base(_dbf) { }

        [CompressResponse]
        public DataSourceDataResponse Any(DataSourceDataRequest request)
        {
            this.Log.Info("data request");

            DataSourceDataResponse dsresponse = null;

            var _ds = this.Redis.Get<EbDataSource>(request.RefId);
            string _sql = string.Empty;

            if (_ds != null)
            {
                string _c = string.Empty;

                if (request.TFilters != null && request.TFilters.Count > 0)
                {
                    foreach (TFilters _dic in request.TFilters)
                    {
                        var op = _dic.Operator; var col = _dic.Column; var val = _dic.Value;

                        if (op == "x*")
                            _c += string.Format("AND LOWER({0})::text LIKE LOWER('{1}%') ", col, val);
                        else if (op == "*x")
                            _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}') ", col, val);
                        else if (op == "*x*")
                            _c += string.Format("AND LOWER({0})::text LIKE LOWER('%{1}%') ", col, val);
                        else if (op == "=")
                            _c += string.Format("AND LOWER({0}::text) = LOWER('{1}') ", col, val);
                        else
                            _c += string.Format("AND {0} {1} '{2}' ", col, op, val);
                    }
                }
                if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
                    _sql = _ds.Sql.Replace("@and_search", _c);
                else
                    _sql = _ds.Sql.Replace(":and_search", _c);
            }
            bool _isPaged = false;
            if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
            {
                _sql = _sql.Replace("@orderby",
                (string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));

                _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));

                //var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
                if (request.Params == null)
                    _sql = _sql.Replace("@id", "0");
            }
            else
            {
                _sql = _sql.Replace(":orderby",
               (string.IsNullOrEmpty(request.OrderByCol)) ? "id" : string.Format("{0} {1}", request.OrderByCol, ((request.OrderByDir == 2) ? "DESC" : "ASC")));

                _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));


                if (request.Params == null)
                    _sql = _sql.Replace(":id", "0");
            }
            var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, request.Length, request.Start);
            Console.WriteLine("Before :  " + DateTime.Now);
            var dtStart = DateTime.Now;
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
            Console.WriteLine("................................................datasourceDSrequeststart " + DateTime.Now);
            var dtstop = DateTime.Now;
            Console.WriteLine("..................................totaltimeinSeconds" + dtstop.Subtract(dtStart).Seconds);

            //-- 
            Console.WriteLine(DateTime.Now);
            var dtEnd = DateTime.Now;
            var ts = (dtEnd - dtStart).TotalMilliseconds;
            Console.WriteLine("final:::" + ts);
            int _recordsTotal = 0, _recordsFiltered = 0;
            if (_isPaged)
            {
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsTotal);
                Int32.TryParse(_dataset.Tables[0].Rows[0][0].ToString(), out _recordsFiltered);
            }
            _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[0].Rows.Count;
            _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[0].Rows.Count;
            //-- 

            dsresponse = new DataSourceDataResponse
            {
                Draw = request.Draw,
                Data = (_dataset.Tables.Count > 1) ? _dataset.Tables[1].Rows : _dataset.Tables[0].Rows,
                RecordsTotal = _recordsTotal,
                RecordsFiltered = _recordsFiltered,
                Ispaged = _isPaged
            };
            this.Log.Info("dsresponse*****" + dsresponse.Data);
            var x = EbSerializers.Json_Serialize(dsresponse);
            return dsresponse;
        }


        [CompressResponse]
        public DataSourceColumnsResponse Any(DataSourceColumnsRequest request)
        {
            string _dsRedisKey = string.Format("{0}_columns", request.RefId);
            EbDataSet _dataset = null;
            bool _isPaged = false;
            DataSourceColumnsResponse resp = null;
            //DataSourceColumnsResponse resp = this.Redis.Get<DataSourceColumnsResponse>(_dsRedisKey);

            if (resp == null || resp.Columns == null || resp.Columns.Count == 0)
            {
                resp = new DataSourceColumnsResponse();
                resp.Columns = new List<ColumnColletion>();
                //EbDataSource _ds = null;
                var _ds = this.Redis.Get<EbDataSource>(request.RefId);
                if (_ds == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataSource>(request.RefId, _ds);
                }

                if (_ds != null)
                {
                    string _sql = string.Empty;
                    if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.PGSQL)
                    {
                        _sql = _ds.Sql/*Decoded()*/.Replace("@and_search", string.Empty).Replace("@orderby", "1");
                        _isPaged = (_sql.ToLower().Contains("@offset") && _sql.ToLower().Contains("@limit"));


                        if (request.Params == null)
                            _sql = _sql.Replace("@id", "0");
                    }
                    else
                    {
                        _sql = _ds.Sql/*Decoded()*/.Replace(":and_search", string.Empty).Replace(":orderby", "1");
                        _isPaged = (_sql.ToLower().Contains(":offset") && _sql.ToLower().Contains(":limit"));


                        if (request.Params == null)
                            _sql = _sql.Replace(":id", "0");
                    }
                    var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params, 0, 0);

                    try
                    {
                        Console.WriteLine("................................................datasourcecolumnrequeststart " + System.DateTime.Now);
                        _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                        Console.WriteLine("................................................datasourcecolumnrequestfinish " + System.DateTime.Now);

                        foreach (var dt in _dataset.Tables)
                            resp.Columns.Add(dt.Columns);

                        resp.IsPaged = _isPaged;
                        this.Redis.Set<DataSourceColumnsResponse>(_dsRedisKey, resp);
                    }
                    catch (Exception e)
                    {
                        Log.Info(">>>>>>>>>>>>>>>>>>>>>>>> dscolumns e.Message: " + e.Message);
                        this.Redis.Remove(_dsRedisKey);
                    }
                }
            }

            return resp;
        }

        [CompressResponse]
        public DataSourceDataSetResponse Any(DataSourceDataSetRequest request)
        {
            this.Log.Info("data request");

            DataSourceDataSetResponse dsresponse = null;

            var _ds = this.Redis.Get<EbDataSource>(request.RefId);
            string _sql = string.Empty;

            if (_ds != null)
            {
                string _c = string.Empty;

                _sql = _ds.Sql;
            }
            List<DbParameter> parameters = new List<DbParameter>();
            if (request.Params != null && request.Params.Count > 0)
            {
                foreach (Param param in request.Params)
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(string.Format("{0}", param.Name), (EbDbTypes)Convert.ToInt32(param.Type), param.Value));
            }
            var _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());

            dsresponse = new DataSourceDataSetResponse
            {
                DataSet = _dataset
            };
            return dsresponse;
        }
    }
}