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
using OfficeOpenXml.DataValidation.Contracts;
using OfficeOpenXml.DataValidation;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
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

            //List<ColumnsInfo> _cols = new List<ColumnsInfo>();
            //MemoryStream ms = new MemoryStream();
            byte[] bytes = null;
            using (var excel = new ExcelPackage())
            {
                ExcelWorksheet workSheet = null;
                if(_form.DisplayName != " " && _form.DisplayName != null && _form.DisplayName != string.Empty)
                {
                    workSheet = excel.Workbook.Worksheets.Add(_form.DisplayName);
                } 
                else
                {
                    workSheet = excel.Workbook.Worksheets.Add(_form.Name);
                }
                int colIndex = 1;
                foreach (var _tbl in _form.FormSchema.Tables)
                {
                    string _tblName = _tbl.TableName;
                    foreach (var _col in _tbl.Columns)
                    {
                        if(_col.Control.Label != null && _col.Control.Label != string.Empty)
                            workSheet.Cells[1, colIndex].Value = _col.Control.Label;
                        else
                            workSheet.Cells[1, colIndex].Value = _col.Control.Name;
                        var headerCells = workSheet.Cells[1, 1, 1, workSheet.Dimension.End.Column];
                        var headerFont = headerCells.Style.Font;
                        headerFont.Bold = true;
                        string comment = JsonConvert.SerializeObject(new ColumnsInfo { Name = _col.Control.Name, Label = _col.Control.Label, DbType = _col.Control.EbDbType, TableName = _tblName });
                        workSheet.Cells[1, colIndex].AddComment(comment, "ExpressBase");
                        var range = ExcelRange.GetAddress(2, colIndex, ExcelPackage.MaxRows, colIndex);
                        if (_col.Control.EbDbType.ToString() == "Decimal")
                        {
                            IExcelDataValidationDecimal validate = workSheet.DataValidations.AddDecimalValidation(range);
                            validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
                            validate.PromptTitle = "Enter a integer value here";
                            validate.Prompt = "Decimal only allowed";
                            validate.ShowInputMessage = true;
                            validate.ErrorTitle = "Invalid Value entered";
                            validate.Error = "This cell must be a valid positive number.";
                            validate.ShowErrorMessage = true;
                            validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
                            validate.Formula.Value = 0D;
                            workSheet.Column(colIndex).Style.Numberformat.Format = "0";
                            validate.AllowBlank = true;
                        }

                        else if (_col.Control.EbDbType.ToString() == "Date")
                        {
                            IExcelDataValidationDateTime validate = workSheet.DataValidations.AddDateTimeValidation(range);
                            validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
                            validate.PromptTitle = "Enter valid date here";
                            validate.Prompt = "YYYY-MM-DD format allowed";
                            validate.ShowInputMessage = true;
                            validate.ErrorTitle = "Invalid Date entered";
                            validate.Error = "This cell must be a date in YYYY-MM-DD format";
                            validate.ShowErrorMessage = true;
                            validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
                            validate.Formula.Value = new DateTime(1800, 01, 01);
                            workSheet.Column(colIndex).Style.Numberformat.Format = "YYYY-MM-DD";
                            validate.AllowBlank = true;
                        }

                        else if (_col.Control.EbDbType.ToString() == "DateTime")
                        {
                            IExcelDataValidationDateTime validate = workSheet.DataValidations.AddDateTimeValidation(range);
                            validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
                            validate.PromptTitle = "Enter valid Date Time";
                            validate.Prompt = "yyyy-MM-dd HH:mm:ss";
                            validate.ShowInputMessage = true;
                            validate.ErrorTitle = "Invalid Date Time entered";
                            validate.Error = "This cell must be a Date Time in yyyy-MM-dd HH:mm:ss format";
                            validate.ShowErrorMessage = true;
                            validate.Operator = ExcelDataValidationOperator.greaterThanOrEqual;
                            validate.Formula.Value = new DateTime(1800, 01, 01);
                            workSheet.Column(colIndex).Style.Numberformat.Format = "yyyy-MM-dd HH:mm:ss";
                            validate.AllowBlank = true;
                        }

                        else if (_col.Control.EbDbType.ToString() == "Boolean")
                        {
                            IExcelDataValidationList validate = workSheet.DataValidations.AddListValidation(range);
                            validate.ErrorStyle = ExcelDataValidationWarningStyle.stop;
                            validate.PromptTitle = "Enter Yes/No";
                            validate.Prompt = "Yes/No";
                            validate.ShowInputMessage = true;
                            validate.ErrorTitle = "Invalid formate";
                            validate.Error = "This cell must be in Yes/No format";
                            validate.ShowErrorMessage = true;
                            validate.Formula.Values.Add("Yes");
                            validate.Formula.Values.Add("No");
                            validate.AllowBlank = false;

                        }
                        else { }
                        //workSheet.Column(colIndex).AutoFit();
                        colIndex++;
                    }
                }
                workSheet.Cells.AutoFitColumns();
                bytes = excel.GetAsByteArray();
                //excel.SaveAs(ms);
            }
            //ms.Position = 0;

            return new ExcelDownloadResponse { stream = bytes, fileName = _form.Name + ".xlsx" };
        }
    }

}
