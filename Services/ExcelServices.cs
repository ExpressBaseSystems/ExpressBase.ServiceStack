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
using System.IO;
using Microsoft.AspNetCore.Mvc;
using ExpressBase.Objects.Objects;
using Newtonsoft.Json;
using ExpressBase.Common.Excel;
using OfficeOpenXml;
using ExpressBase.Common.ServiceClients;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class ExcelServices : EbBaseService
    {
        //public ExcelServices(IEbConnectionFactory _dbf) : base() { }
        public ExcelServices(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc) : base(_dbf, _sfc) { }

        public ExcelDownloadResponse Get(ExcelDownloadRequest request)
        {
            ExcelDownloadResponse response = new ExcelDownloadResponse();

            //.......Get Webform obj ........
            //EbWebForm _form = this.Redis.Get<EbWebForm>(request._refid);
            //if (_form == null)
            //{
            //    var myService = base.ResolveService<EbObjectService>();
            //    EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request._refid });
            //    _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
            //    this.Redis.Set<EbWebForm>(request._refid, _form);
            //}
            //_form.AfterRedisGet(this);

            ////List<ColumnsInfo> _cols = new List<ColumnsInfo>();
            ////MemoryStream ms = new MemoryStream();
            //byte[] bytes = null;
            //string _worksheetName = string.IsNullOrEmpty(_form.DisplayName.Trim()) ? _form.Name.Trim() : _form.DisplayName.Trim();
            //using (var excel = new ExcelPackage())
            //{
            //    ExcelWorksheet workSheet = null;
            //    workSheet = excel.Workbook.Worksheets.Add(_worksheetName);
            //    int colIndex = 1;
            //    foreach (var _tbl in _form.FormSchema.Tables)
            //    {
            //        string _tblName = _tbl.TableName;
            //        foreach (var _col in _tbl.Columns)
            //        {
            //            if (_col.Control.ObjType != "ProvisionUser")
            //            {
            //                if (_col.Control.Label != null && _col.Control.Label != string.Empty)
            //                    workSheet.Cells[1, colIndex].Value = _col.Control.Label;
            //                else
            //                    workSheet.Cells[1, colIndex].Value = _col.Control.Name;
            //                var headerCells = workSheet.Cells[1, 1, 1, workSheet.Dimension.End.Column];
            //                var headerFont = headerCells.Style.Font;
            //                headerFont.Bold = true;
            //                string comment = JsonConvert.SerializeObject(new ColumnsInfo { Name = _col.Control.Name, Label = _col.Control.Label, DbType = _col.Control.EbDbType, TableName = _tblName });
            //                workSheet.Cells[1, colIndex].AddComment(comment, "ExpressBase");
            //                var range = ExcelRange.GetAddress(2, colIndex, ExcelPackage.MaxRows, colIndex);
            //                if (_col.Control.ObjType == "PowerSelect")
            //                {
            //                    if ((_col.Control as EbPowerSelect).ParamsList == null || (_col.Control as EbPowerSelect).ParamsList.Count == 0)
            //                    {
            //                        ExcelWorksheet ws = excel.Workbook.Worksheets.Add(_col.Control.Name);
            //                        GetWorksheet(ws, _col);
            //                        IExcelDataValidationList validate = workSheet.DataValidations.AddListValidation(range);
            //                        validate.Formula.ExcelFormula = $"={_col.Control.Name}!$A$2:$A${ws.Dimension.End.Row}";
            //                        validate.ShowErrorMessage = true;
            //                        validate.Error = "Select from List of Values ...";
            //                        validate.AllowBlank = false;
            //                        var arr = range.Split(':');
            //                        var char1 = arr[0][0];
            //                        var num1 = arr[0].Trim(char1);
            //                        var char2 = arr[1][0];
            //                        var num2 = arr[1].Trim(char2);
            //                        colIndex++;
            //                        workSheet.Cells[1, colIndex].Value = (_col.Control as EbPowerSelect).ValueMember.Name;
            //                        workSheet.Column(colIndex).Hidden = false;
            //                        var _formula = $"=VLOOKUP({_worksheetName}!${char1}${num1}:${char2}${num2}, {_col.Control.Name}!$A$2:$B${ws.Dimension.End.Row}, 2, 0)";
            //                        workSheet.Cells[ExcelRange.GetAddress(2, colIndex, ExcelPackage.MaxRows, colIndex)].Formula = _formula;
            //                        //IExcelDataValidationCustom validate1 = workSheet.DataValidations.AddCustomValidation(ExcelRange.GetAddress(2, colIndex, ExcelPackage.MaxRows, colIndex));
            //                        //validate1.Formula.ExcelFormula = _formula;
            //                        //workSheet.Cells[ExcelRange.GetAddress(2, colIndex, ExcelPackage.MaxRows, colIndex)].FormulaR1C1 = "CONCATENATE( RC[-1] , RC[1] )";
            //                    }
            //                }
            //                else if (_col.Control.EbDbType.ToString() == "Decimal")
            //                {
            //                    IExcelDataValidationDecimal validate = workSheet.DataValidations.AddDecimalValidation(range);
            //                    validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                    validate.PromptTitle = "Enter a integer value here";
            //                    validate.Prompt = "Decimal only allowed";
            //                    validate.ShowInputMessage = true;
            //                    validate.ErrorTitle = "Invalid Value entered";
            //                    validate.Error = "This cell must be a valid positive number.";
            //                    validate.ShowErrorMessage = true;
            //                    validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
            //                    validate.Formula.ExcelFormula = "SUM(10,10)";
            //                    workSheet.Column(colIndex).Style.Numberformat.Format = "0";
            //                    validate.AllowBlank = true;
            //                }

            //                else if (_col.Control.EbDbType.ToString() == "Date")
            //                {
            //                    IExcelDataValidationDateTime validate = workSheet.DataValidations.AddDateTimeValidation(range);
            //                    validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                    validate.PromptTitle = "Enter valid date here";
            //                    validate.Prompt = "YYYY-MM-DD format allowed";
            //                    validate.ShowInputMessage = true;
            //                    validate.ErrorTitle = "Invalid Date entered";
            //                    validate.Error = "This cell must be a date in YYYY-MM-DD format";
            //                    validate.ShowErrorMessage = true;
            //                    validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
            //                    validate.Formula.Value = new DateTime(1800, 01, 01);
            //                    workSheet.Column(colIndex).Style.Numberformat.Format = "YYYY-MM-DD";
            //                    validate.AllowBlank = true;
            //                }

            //                else if (_col.Control.EbDbType.ToString() == "DateTime")
            //                {
            //                    IExcelDataValidationDateTime validate = workSheet.DataValidations.AddDateTimeValidation(range);
            //                    validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                    validate.PromptTitle = "Enter valid Date Time";
            //                    validate.Prompt = "yyyy-MM-dd HH:mm:ss";
            //                    validate.ShowInputMessage = true;
            //                    validate.ErrorTitle = "Invalid Date Time entered";
            //                    validate.Error = "This cell must be a Date Time in yyyy-MM-dd HH:mm:ss format";
            //                    validate.ShowErrorMessage = true;
            //                    validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
            //                    validate.Formula.Value = new DateTime(1800, 01, 01);
            //                    workSheet.Column(colIndex).Style.Numberformat.Format = "yyyy-MM-dd HH:mm:ss";
            //                    validate.AllowBlank = true;
            //                }

            //                else if (_col.Control.EbDbType.ToString() == "BooleanOriginal")
            //                {
            //                    IExcelDataValidationList validate = workSheet.DataValidations.AddListValidation(range);
            //                    validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                    validate.PromptTitle = "Enter Yes/No";
            //                    validate.Prompt = "Yes/No";
            //                    validate.ShowInputMessage = true;
            //                    validate.ErrorTitle = "Invalid formate";
            //                    validate.Error = "This cell must be in Yes/No format";
            //                    validate.ShowErrorMessage = true;
            //                    validate.Formula.Values.Add("Yes");
            //                    validate.Formula.Values.Add("No");
            //                    validate.AllowBlank = false;

            //                }

            //                else if (_col.Control.ObjType == "RadioButton")
            //                {

            //                    if (_col.Control.EbDbType.ToString() == "Int32")
            //                    {
            //                        EbRadioButton _control = _col.Control as EbRadioButton;
            //                        IExcelDataValidationList validate = workSheet.DataValidations.AddListValidation(range);
            //                        validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                        validate.PromptTitle = "Enter boolean";
            //                        validate.Prompt = "Choose bool from list";
            //                        validate.ShowInputMessage = true;
            //                        validate.ErrorTitle = "Invalid formate";
            //                        validate.Error = "Invalid bool val";
            //                        validate.ShowErrorMessage = true;
            //                        validate.Formula.Values.Add(_control.TrueValue_I.ToString());
            //                        validate.Formula.Values.Add(_control.FalseValue_I.ToString());
            //                        validate.AllowBlank = false;
            //                    }
            //                    else if (_col.Control.EbDbType.ToString() == "String" || _col.Control.EbDbType.ToString() == "Boolean")
            //                    {
            //                        IExcelDataValidationList validate = workSheet.DataValidations.AddListValidation(range);
            //                        validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
            //                        validate.PromptTitle = "Enter Yes/No";
            //                        validate.Prompt = "Yes/No";
            //                        validate.ShowInputMessage = true;
            //                        validate.ErrorTitle = "Invalid formate";
            //                        validate.Error = "This cell must be in Yes/No format";
            //                        validate.ShowErrorMessage = true;
            //                        validate.Formula.Values.Add("Yes");
            //                        validate.Formula.Values.Add("No");
            //                        validate.AllowBlank = false;

            //                    }
            //                }
            //                //workSheet.Column(colIndex).AutoFit();
            //                colIndex++;
            //            }
            //        }
            //    }
            //    workSheet.Cells.AutoFitColumns();
            //    //workSheet.Calculate();
            //    bytes = excel.GetAsByteArray();
            //    //excel.SaveAs(ms);
            //}
            //ms.Position = 0;

            //return new ExcelDownloadResponse { stream = bytes, fileName = _worksheetName + ".xlsx" };
            return response;
        }

        //public void GetWorksheet(ExcelWorksheet ws, ColumnSchema _col)
        //{
        //    string dis_name = (_col.Control as EbPowerSelect).DisplayMembers[0].Name;
        //    string val_name = (_col.Control as EbPowerSelect).ValueMember.Name;
        //    Dictionary<int, string> dict = this.EbConnectionFactory.ObjectsDB.GetDictionary((_col.Control as EbPowerSelect).GetSql(this), dis_name, val_name);
        //    int count = 2;
        //    ws.Cells[1, 1].Value = dis_name;
        //    ws.Cells[1, 2].Value = val_name;
        //    foreach( KeyValuePair<int, string> keyValue in dict)
        //    {
        //        ws.Cells[count, 1].Value = keyValue.Value;
        //        ws.Cells[count++, 2].Value = keyValue.Key;
        //    }  
        //}
    }

}
