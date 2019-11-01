using ExpressBase.Common;
using ExpressBase.Security;
using ExpressBase.Common.Data;
using ExpressBase.Common.Singletons;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ExpressBase.Objects.Objects;
using System.Globalization;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class CalendarService : EbBaseService
    {
        public CalendarService(IEbConnectionFactory _dbf) : base(_dbf) { }

        private ResponseStatus _Responsestatus = new ResponseStatus();

        private User _user = new User();

        private string PreviousGroupingText = string.Empty;

        private EbDataSet dataset = new EbDataSet();

        private EbDataTable _formattedTable = null;

        private EbCalendarView _dV = null;

        private int j = -1;

        private CultureInfo _user_culture = null;

        private bool Modifydv = true;

        [CompressResponse]
        public CalendarDataResponse Any(CalendarDataRequest request)
        {
            try
            {
                this.Log.Info("data request");
                Modifydv = request.ModifyDv;
                _dV = request.CalendarObj;
                this._user = request.UserInfo;

                CalendarDataResponse dsresponse = null;
                var _ds = this.Redis.Get<EbDataReader>(request.RefId);

                if (_ds == null)
                {
                    var myService = base.ResolveService<EbObjectService>();
                    var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                    _ds = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbDataReader>(request.RefId, _ds);
                }
                if (_ds.FilterDialogRefId != string.Empty)
                {
                    var _dsf = this.Redis.Get<EbFilterDialog>(_ds.FilterDialogRefId);
                    if (_dsf == null)
                    {
                        var myService = base.ResolveService<EbObjectService>();
                        var result = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = _ds.FilterDialogRefId });
                        _dsf = EbSerializers.Json_Deserialize(result.Data[0].Json);
                        Redis.Set<EbFilterDialog>(_ds.FilterDialogRefId, _dsf);
                    }
                    if (request.Params == null)
                        request.Params = _dsf.GetDefaultParams();
                }
                string _sql = string.Empty;
                string tempsql = string.Empty;

                bool _isPaged = false;
                if (_ds != null)
                {
                    string _c = string.Empty;
                    _sql = _ds.Sql;
                }
                var parameters = DataHelper.GetParams(this.EbConnectionFactory, _isPaged, request.Params,0, 0);
                EbDataSet _dataset = null;
                try
                {
                    _dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(_sql, parameters.ToArray<System.Data.Common.DbParameter>());
                }
                catch (Exception e)
                {
                    Log.Info("Datviz Qurey Exception........." + e.StackTrace);
                    Log.Info("Datviz Qurey Exception........." + e.Message);
                    this._Responsestatus.Message = e.Message;
                }
                if (GetLogEnabled(request.RefId))
                {
                    TimeSpan T = _dataset.EndTime - _dataset.StartTime;
                    InsertExecutionLog(_dataset.RowNumbers, T, _dataset.StartTime, request.UserId, request.Params, request.RefId);
                }
                int _recordsTotal = 0, _recordsFiltered = 0;
                if (_isPaged)
                {
                    Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsTotal);
                    Int32.TryParse(_dataset.Tables[_dataset.Tables.Count - 1].Rows[0][0].ToString(), out _recordsFiltered);
                }
                _recordsTotal = (_recordsTotal > 0) ? _recordsTotal : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
                _recordsFiltered = (_recordsFiltered > 0) ? _recordsFiltered : _dataset.Tables[_dataset.Tables.Count - 1].Rows.Count;
                //-- 
                PrePrcessorReturn ReturnObj = null;
                if (_dataset.Tables.Count > 0 && _dV != null)
                {
                    ReturnObj = PreProcessing(_dataset, request.Params);
                }

                List<string> _permission = new List<string>();
                dsresponse = new CalendarDataResponse
                {
                    Data = (ReturnObj?.rows != null) ? ReturnObj.rows : _dataset.Tables[0].Rows,
                    FormattedData = (ReturnObj?.FormattedTable != null) ? ReturnObj.FormattedTable.Rows : null,
                    CalendarReturnObj = _dV,
                    ResponseStatus = this._Responsestatus
                };
                this.Log.Info(" dataviz dataresponse*****" + dsresponse.Data);
                EbSerializers.Json_Serialize(dsresponse);
                return dsresponse;
            }
            catch (Exception e)
            {
                Log.Info("Datviz service Exception........." + e.StackTrace);
                Log.Info("Datviz service Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
            return null;
        }

        public PrePrcessorReturn PreProcessing(EbDataSet _dataset, List<Param> Parameters)
        {
            try
            {
                this._user_culture = CultureHelper.GetSerializedCultureInfo(this._user.Preference.Locale).GetCultureInfo();

                var colCount = _dataset.Tables[0].Columns.Count;

                Globals globals = new Globals();
                if (Modifydv)
                    this.CreateCustomcolumn(ref _dataset, Parameters);
                else
                    this.CreateCustomcolumn4EbDataSet(ref _dataset, Parameters);
                _formattedTable = _dataset.Tables[0].GetEmptyTable();
                _formattedTable.Columns.Add(_formattedTable.NewDataColumn(_dV.Columns.Count, "serial", EbDbTypes.Int32));
                RowColletion rows = _dataset.Tables[0].Rows;
                for (int i = 0; i < rows.Count; i++)
                {
                    CreateFormattedTable(rows[i], i );
                }
                return new PrePrcessorReturn { FormattedTable = _formattedTable, rows = rows };
            }
            catch (Exception e)
            {
                Log.Info("Before PreProcessing in datatable  Exception........." + e.StackTrace);
                Log.Info("Before PreProcessing in datatable  Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }
            return null;
        }

        public void CreateCustomcolumn(ref EbDataSet _dataset, List<Param> Parameters)
        {           
            var dates = new List<DateTime>();
            int index = _dV.Columns.Count;
            for (var date = new DateTime(2015, 3, 1); date.Month == 3; date = date.AddDays(1))
            {
                _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = date.Date.ToString(), Type = EbDbTypes.String });
                _dV.Columns.Add(new DVStringColumn { Data = index++, Name = date.ToString("dd-MM-yyyy"), sTitle = date.ToString("ddd")[0] + "</br>" + date.ToString("dd"), Type = EbDbTypes.String, IsCustomColumn = true , bVisible=true});
            }
        }

        public void CreateCustomcolumn4EbDataSet(ref EbDataSet _dataset, List<Param> Parameters)
        {
            var dates = new List<DateTime>();
            int index = _dV.Columns.Count;
            for (var date = new DateTime(2015, 3, 1); date.Month == 3; date = date.AddDays(1))
            {
                _dataset.Tables[0].Columns.Add(new EbDataColumn { ColumnIndex = index, ColumnName = date.Date.ToString(), Type = EbDbTypes.String });
            }
        }

        public void CreateFormattedTable(EbDataRow row, int i)
        {
            try
            {
                object TempGroupingText = row[2].ToString();//keycolumn
                if (TempGroupingText.Equals(PreviousGroupingText) == false)
                {
                    j++;
                    PreviousGroupingText = TempGroupingText.ToString();
                    _formattedTable.Rows.Add(_formattedTable.NewDataRow2());
                    _formattedTable.Rows[j][_formattedTable.Columns.Count - 1] = j + 1;//serial
                    object unformated = Convert.ToDateTime( row[7]).ToString("dd-MM-yyyy");// date column date
                    foreach (DVBaseColumn col in _dV.Columns)
                    {
                        var cults = col.GetColumnCultureInfo(this._user_culture);
                        if (col.IsCustomColumn)
                        {
                            if(unformated.ToString() == col.Name)
                            {
                                _formattedTable.Rows[j][col.Data] = row[4].ToString() + " , " + row[5].ToString() + " , " + row[6].ToString();
                            }
                        }
                        else
                        {
                            if (col.Type == EbDbTypes.Date || col.Type == EbDbTypes.DateTime)
                                _formattedTable.Rows[j][col.Data] = Convert.ToDateTime(row[col.Data]).ToString("d", cults.DateTimeFormat); 
                            else
                                _formattedTable.Rows[j][col.Data] = row[col.Data];

                        }
                    }
                }
                else
                {
                    object unformated = Convert.ToDateTime(row[7]).ToString("dd-MM-yyyy");// date column
                    foreach (DVBaseColumn col in _dV.Columns)
                    {
                        if (col.IsCustomColumn)
                        {
                            if (unformated.ToString() == col.Name)
                            {
                                _formattedTable.Rows[j][col.Data] = row[4].ToString() + " , " + row[5].ToString() + " , " + row[6].ToString();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("PreProcessing in datatable Exception........." + e.StackTrace);
                Log.Info("PreProcessing in datatable Exception........." + e.Message);
                this._Responsestatus.Message = e.Message;
            }

        }
    }
}
