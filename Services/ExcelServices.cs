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
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Text.RegularExpressions;
using DocumentFormat.OpenXml;
using System.Xml;
using System.Text;
using System.Reflection;

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
            EbWebForm _form = this.Redis.Get<EbWebForm>(request._refid);
            if (_form == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request._refid });
                _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbWebForm>(request._refid, _form);
            }
            _form.AfterRedisGet_All(this);

            //List<ColumnsInfo> _cols = new List<ColumnsInfo>();
            //MemoryStream ms = new MemoryStream();
            byte[] bytes = null;
            var matches = Regex.Matches(_form.DisplayName.Trim(), @"[^\u0000-\u007F]+");
            string _worksheetName = string.IsNullOrEmpty(_form.DisplayName.Trim()) || matches.Count > 0 ? _form.Name.Trim().Replace(" ", "") : _form.DisplayName.Trim().Replace(" ", "");

            MemoryStream ms = new MemoryStream();

            var document = SpreadsheetDocument.Create(ms, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook);
            WorkbookPart workbookPart = document.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();

            WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>("rId1");
            worksheetPart.Worksheet = new Worksheet();
            Sheets sheets1 = new Sheets();
            Sheet sheet1 = new Sheet() { Name = _worksheetName, SheetId = (UInt32Value)1U, Id = document.WorkbookPart.GetIdOfPart(worksheetPart) };
            sheets1.Append(sheet1);
            SheetData sheetData = new SheetData();
            worksheetPart.Worksheet.Append(sheetData);
            worksheetPart.Worksheet.Save();
            Dictionary<string, string> commentsdict = new Dictionary<string, string>();
            DataValidations newDVs = new DataValidations();
            WorkbookStylesPart stylesheet = workbookPart.AddNewPart<WorkbookStylesPart>();
            stylesheet.Stylesheet = GenerateStylesheet();
            stylesheet.Stylesheet.Save();
            var type = typeof(IEbPlaceHolderControl);
            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(s => s.GetTypes())
                .Where(p => type.IsAssignableFrom(p)).ToList();
            types.Remove(type);
            var NonExcelcontrol = types.Map(ss => Activator.CreateInstance(ss)).ToList();
            List<object> NonExcelcontrollist = new List<object>();
            NonExcelcontrollist.Add(new EbFileUploader());
            NonExcelcontrollist.Add(new EbDataGrid());
            NonExcelcontrollist.Add(new EbSimpleFileUploader());
            NonExcelcontrollist.Add(new EbDisplayPicture());
            bool isPowerselectAdded = false;
            int powerselectCount = 0;
            foreach (var _tbl in _form.FormSchema.Tables)
            {
                Row row = new Row();
                if (sheetData.Elements<Row>().Count() == 0 && _tbl.Columns.Count > 0)
                    sheetData.Append(row);
                int colIndex = 1;
                Columns columns = new Columns();
                string _tblName = _tbl.TableName;
                var ExcelControl = _tbl.Columns.Select(ff => ff.Control).Where(xx => !NonExcelcontrol.Select(pp => (pp as EbControl).ObjType).ToList().Contains(xx.ObjType));
                ExcelControl = ExcelControl.Where(control => !NonExcelcontrollist.Select(pp => (pp as EbControl).ObjType).ToList().Contains(control.ObjType));
                int ColumnCount = ExcelControl.Count();
                foreach (var _col in ExcelControl)
                {
                    EbControl control = _col as EbControl;
                    columns.InsertAt(new Column() { Min = Convert.ToUInt32(colIndex), Max = Convert.ToUInt32(colIndex), Width = 25, CustomWidth = true }, colIndex - 1);
                    Cell cell = new Cell();
                    cell.StyleIndex = 0;
                    cell.DataType = CellValues.String;
                    if (!string.IsNullOrEmpty(control.Label))
                        cell.CellValue = new CellValue(control.Label);
                    else
                        cell.CellValue = new CellValue(control.Name);
                    string comment = JsonConvert.SerializeObject(new ColumnsInfo { Name = control.Name, Label = control.Label, DbType = control.EbDbType, TableName = _tblName, ControlType = control.ObjType });
                    //workSheet.Cells[1, colIndex].AddComment(comment, "ExpressBase");
                    var cellref = GetExcelColumnName(colIndex) + 1;
                    cell.CellReference = cellref;
                    commentsdict.Add(cellref, comment);
                    DataValidation dataValidation = new DataValidation
                    {
                        AllowBlank = true,
                        SequenceOfReferences = new ListValue<StringValue>() { InnerText = $"{GetExcelColumnName(colIndex)}2:{GetExcelColumnName(colIndex)}1048576" },
                        Type = DataValidationValues.Custom,
                        ShowErrorMessage = true,
                        ShowInputMessage = true,
                        ErrorStyle = DataValidationErrorStyleValues.Stop,
                        Prompt = "Valid Item only allowed",
                        PromptTitle = "Enter a valid value here",
                        ErrorTitle = "Invalid Value entered",
                        Error = "This cell must be a valid value."
                    };

                    if (control.ObjType == "PowerSelect" || control.ObjType == "SimpleSelect")
                    {
                        if (control.ObjType == "PowerSelect")
                        {
                            if ((control as EbPowerSelect).ParamsList != null && (control as EbPowerSelect).ParamsList.Count > 0)
                            {
                                colIndex++;
                                continue;
                            }
                        }
                        powerselectCount++;
                        WorksheetPart worksheetPart2 = workbookPart.AddNewPart<WorksheetPart>("rId" + powerselectCount + 1);
                        worksheetPart2.Worksheet = new Worksheet();
                        uint sid = (uint)(1 + powerselectCount);
                        Sheet sheet2 = new Sheet() { Name = cell.CellValue.InnerText.Trim().Replace(" ", ""), SheetId = sid, Id = document.WorkbookPart.GetIdOfPart(worksheetPart2) };
                        sheets1.Append(sheet2);
                        SheetData sheetData2 = new SheetData();
                        worksheetPart2.Worksheet.Append(sheetData2);
                        CreateWorksheet4Ps(sheetData2, control);
                        //worksheetPart2.Worksheet.Save();  
                        dataValidation.Type = DataValidationValues.List;
                        dataValidation.Append(
                        new Formula1(string.Format("'{0}'!$A$2:$A${1}", sheet2.Name, sheetData2.ChildElements.Count + 1))
                        );
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                        ColumnCount++;
                        string lokkRange = $"{_worksheetName}!${GetExcelColumnName(colIndex)}$2:${GetExcelColumnName(colIndex)}$1048576";
                        string array = $"{sheet2.Name}!$A$2:$B${sheetData2.ChildElements.Count + 1}";
                        //row.Append(cell1);
                        for (int j = 2; j < 1000; j++)
                        {
                            Row rr = new Row();
                            if (isPowerselectAdded)
                                rr = sheetData.Elements<Row>().ElementAt(j - 1);
                            else
                                sheetData.Append(rr);
                            Cell cell2 = new Cell();
                            cell2.StyleIndex = 0;
                            cell2.DataType = CellValues.String;
                            //cell2.CellValue = new CellValue("0");
                            cell2.CellReference = GetExcelColumnName(ColumnCount) + j;
                            CellFormula cellformula = new CellFormula();
                            cellformula.Text = $"=IFERROR(VLOOKUP({lokkRange},{array},2,FALSE),\"\")";
                            cell2.Append(cellformula);
                            rr.Append(cell2);
                        }
                        isPowerselectAdded = true;
                    }

                    else if (control.EbDbType.ToString() == "Decimal")
                    {
                        cell.StyleIndex = 3;
                        dataValidation.Type = DataValidationValues.Decimal;
                        //cell.DataType = CellValues.Number;
                        dataValidation.Prompt = "Decimal only allowed";
                        dataValidation.PromptTitle = "Enter a integer value here";
                        dataValidation.ErrorTitle = "Invalid Value entered";
                        dataValidation.Error = "This cell must be a valid number.";
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    else if (control.EbDbType.ToString() == "Date")
                    {
                        cell.StyleIndex = 2;
                        dataValidation.Type = DataValidationValues.Date;
                        //cell.DataType = CellValues.Date;
                        dataValidation.Prompt = "Date only allowed";
                        dataValidation.PromptTitle = "Enter valid date here";
                        dataValidation.ErrorTitle = "Invalid Date  entered";
                        dataValidation.Error = "This cell must be a date ";
                        //dataValidation.Formula1 = new Formula1(new DateTime(1800, 01, 01).ToString());
                        dataValidation.Operator = DataValidationOperatorValues.GreaterThanOrEqual;
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    else if (control.EbDbType.ToString() == "DateTime")
                    {
                        cell.StyleIndex = 1;
                        dataValidation.Type = DataValidationValues.Date;
                        //cell.DataType = CellValues.Date;
                        dataValidation.Prompt = "Date only allowed";
                        dataValidation.PromptTitle = "Enter valid date here";
                        dataValidation.ErrorTitle = "Invalid Date  entered";
                        dataValidation.Error = "This cell must be a date ";
                        dataValidation.Operator = DataValidationOperatorValues.GreaterThanOrEqual;
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    else if (control.EbDbType.ToString() == "Time")
                    {
                        dataValidation.Type = DataValidationValues.Time;
                        //cell.DataType = CellValues.Date;
                        dataValidation.Prompt = "Time only allowed";
                        dataValidation.PromptTitle = "Enter valid Time here";
                        dataValidation.ErrorTitle = "Invalid Time  entered";
                        dataValidation.Error = "This cell must be a Time ";
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    else if (control.EbDbType.ToString() == "BooleanOriginal")
                    {
                        dataValidation.Type = DataValidationValues.List;
                        string vals = "Yes,No";
                        dataValidation.Formula1 = new Formula1("\"" + vals + "\"");
                        //cell.DataType = CellValues.InlineString;
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    else if (control.ObjType == "RadioButton")
                    {
                        EbRadioButton _control = control as EbRadioButton;
                        dataValidation.Type = DataValidationValues.List;
                        string vals = "Yes,No";//Boolean
                        if (control.EbDbType.ToString() == "String")
                            vals = _control.TrueValue_S.ToString() + "," + _control.FalseValue_S.ToString();
                        else if (control.EbDbType.ToString() == "Int32")
                            vals = _control.TrueValue_I.ToString() + "," + _control.FalseValue_I.ToString();
                        dataValidation.Formula1 = new Formula1("\"" + vals + "\"");
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }
                    else
                    {
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                    }

                    colIndex++;
                    newDVs.Count = (newDVs.Count == null) ? 1 : newDVs.Count + 1;
                }
                if (_tbl.Columns.Count > 0)
                {
                    for (int i = _tbl.Columns.Count + 1; i <= ColumnCount; i++)
                        columns.Append(new Column() { Min = Convert.ToUInt32(i), Max = Convert.ToUInt32(i), Hidden = true });
                    worksheetPart.Worksheet.InsertBefore<Columns>(columns, sheetData);
                    break;
                }
            }
            if (isPowerselectAdded)
            {
                powerselectCount++;
                WorksheetPart worksheetPart2 = workbookPart.AddNewPart<WorksheetPart>("rId" + powerselectCount + 1);
                worksheetPart2.Worksheet = new Worksheet();
                uint sid = (uint)(1 + powerselectCount);
                Sheet sheet2 = new Sheet() { Name = "Help", SheetId = sid, Id = document.WorkbookPart.GetIdOfPart(worksheetPart2) };
                sheets1.Append(sheet2);
                SheetData sheetData2 = new SheetData();
                worksheetPart2.Worksheet.Append(sheetData2);
            }
            workbookPart.Workbook.Append(sheets1);
            worksheetPart.Worksheet.Append(newDVs);
            InsertComments(worksheetPart, commentsdict);
            //worksheetPart.Worksheet.Save();
            workbookPart.Workbook.Save();
            document.Close();
            //workSheet.Cells.AutoFitColumns();
            //workSheet.Calculate();
            //bytes = excel.GetAsByteArray();
            //excel.SaveAs(ms);
            ms.Position = 0;
            bytes = ms.ToArray();
            return new ExcelDownloadResponse { stream = bytes, fileName = _worksheetName};
            //return response;
        }

        private void CreateWorksheet4Ps(SheetData sheetdata, EbControl _col)
        {
            string dis_name = string.Empty;
            string val_name = string.Empty;
            Dictionary<int, string> dict = new Dictionary<int, string>();
            if (_col.ObjType == "SimpleSelect")
            {
                dis_name = (_col as EbSimpleSelect).DisplayMember.Name;
                val_name = (_col as EbSimpleSelect).ValueMember.Name;
                dict = this.EbConnectionFactory.ObjectsDB.GetDictionary((_col as EbSimpleSelect).GetSql(this), dis_name, val_name);
            }
            else
            {
                dis_name = (_col as EbPowerSelect).DisplayMembers[0].Name;
                val_name = (_col as EbPowerSelect).ValueMember.Name;
                dict = this.EbConnectionFactory.ObjectsDB.GetDictionary((_col as EbPowerSelect).GetSql(this), dis_name, val_name);
            }
            

            Row headerrow = new Row();
            headerrow.Append(CreateCell(dis_name));
            headerrow.Append(CreateCell(val_name));
            sheetdata.Append(headerrow);
            foreach (KeyValuePair<int, string> keyValue in dict)
            {
                Row innerrow = new Row();
                innerrow.Append(CreateCell(keyValue.Value));
                innerrow.Append(CreateCell(keyValue.Key.ToString()));
                sheetdata.Append(innerrow);
            }
        }

        private static Stylesheet GenerateStylesheet()
        {
            Stylesheet ss = new Stylesheet();

            Fonts fts = new Fonts();
            DocumentFormat.OpenXml.Spreadsheet.Font ft = new DocumentFormat.OpenXml.Spreadsheet.Font();
            FontName ftn = new FontName();
            ftn.Val = "Calibri";
            FontSize ftsz = new FontSize();
            ftsz.Val = 11;
            ft.FontName = ftn;
            ft.FontSize = ftsz;
            fts.Append(ft);
            fts.Count = (uint)fts.ChildElements.Count;

            Fills fills = new Fills();
            Fill fill;
            PatternFill patternFill;
            fill = new Fill();
            patternFill = new PatternFill();
            patternFill.PatternType = PatternValues.None;
            fill.PatternFill = patternFill;
            fills.Append(fill);
            fill = new Fill();
            patternFill = new PatternFill();
            patternFill.PatternType = PatternValues.Gray125;
            fill.PatternFill = patternFill;
            fills.Append(fill);
            fills.Count = (uint)fills.ChildElements.Count;

            Borders borders = new Borders();
            Border border = new Border();
            border.LeftBorder = new LeftBorder();
            border.RightBorder = new RightBorder();
            border.TopBorder = new TopBorder();
            border.BottomBorder = new BottomBorder();
            border.DiagonalBorder = new DiagonalBorder();
            borders.Append(border);
            borders.Count = (uint)borders.ChildElements.Count;

            CellStyleFormats csfs = new CellStyleFormats();
            CellFormat cf = new CellFormat();
            cf.NumberFormatId = 0;
            cf.FontId = 0;
            cf.FillId = 0;
            cf.BorderId = 0;
            csfs.Append(cf);
            csfs.Count = (uint)csfs.ChildElements.Count;

            uint iExcelIndex = 164;
            NumberingFormats nfs = new NumberingFormats();
            CellFormats cfs = new CellFormats();

            cf = new CellFormat();
            cf.NumberFormatId = 0;
            cf.FontId = 0;
            cf.FillId = 0;
            cf.BorderId = 0;
            cf.FormatId = 0;
            cfs.Append(cf);

            NumberingFormat nf;
            nf = new NumberingFormat();
            nf.NumberFormatId = iExcelIndex++;
            nf.FormatCode = "yyyy-MM-dd HH:mm:ss";
            nfs.Append(nf);
            cf = new CellFormat();
            cf.NumberFormatId = nf.NumberFormatId;
            cf.FontId = 0;
            cf.FillId = 0;
            cf.BorderId = 0;
            cf.FormatId = 0;
            cf.ApplyNumberFormat = true;
            cfs.Append(cf);

            nf = new NumberingFormat();
            nf.NumberFormatId = iExcelIndex++;
            nf.FormatCode = "YYYY-MM-DD";
            nfs.Append(nf);
            cf = new CellFormat();
            cf.NumberFormatId = nf.NumberFormatId;
            cf.FontId = 0;
            cf.FillId = 0;
            cf.BorderId = 0;
            cf.FormatId = 0;
            cf.ApplyNumberFormat = true;
            cfs.Append(cf);

            nf = new NumberingFormat();
            nf.NumberFormatId = iExcelIndex++;
            nf.FormatCode = "0";
            nfs.Append(nf);
            cf = new CellFormat();
            cf.NumberFormatId = nf.NumberFormatId;
            cf.FontId = 0;
            cf.FillId = 0;
            cf.BorderId = 0;
            cf.FormatId = 0;
            cf.ApplyNumberFormat = true;
            cfs.Append(cf);

            nfs.Count = (uint)nfs.ChildElements.Count;
            cfs.Count = (uint)cfs.ChildElements.Count;

            ss.Append(nfs);
            ss.Append(fts);
            ss.Append(fills);
            ss.Append(borders);
            ss.Append(csfs);
            ss.Append(cfs);

            CellStyles css = new CellStyles();
            CellStyle cs = new CellStyle();
            cs.Name = "Normal";
            cs.FormatId = 0;
            cs.BuiltinId = 0;
            css.Append(cs);
            css.Count = (uint)css.ChildElements.Count;
            ss.Append(css);

            DifferentialFormats dfs = new DifferentialFormats();
            dfs.Count = 0;
            ss.Append(dfs);

            TableStyles tss = new TableStyles();
            tss.Count = 0;
            tss.DefaultTableStyle = "TableStyleMedium9";
            tss.DefaultPivotStyle = "PivotStyleLight16";
            ss.Append(tss);

            return ss;
        }

        private string GetExcelColumnName(int columnNumber)
        {
            int dividend = columnNumber;
            string columnName = String.Empty;
            int modulo;

            while (dividend > 0)
            {
                modulo = (dividend - 1) % 26;
                columnName = Convert.ToChar(65 + modulo).ToString() + columnName;
                dividend = (int)((dividend - modulo) / 26);
            }

            return columnName;
        }

        /// <summary>
        /// Adds all the comments defined in the commentsToAddDict dictionary to the worksheet
        /// </summary>
        /// <param name="worksheetPart">Worksheet Part</param>
        /// <param name="commentsToAddDict">Dictionary of cell references as the key (ie. A1) and the comment text as the value</param>
        public void InsertComments(WorksheetPart worksheetPart, Dictionary<string, string> commentsToAddDict)
        {
            if (commentsToAddDict.Any())
            {
                string commentsVmlXml = string.Empty;

                // Create all the comment VML Shape XML
                foreach (var commentToAdd in commentsToAddDict)
                {
                    commentsVmlXml += GetCommentVMLShapeXML(GetColumnName(commentToAdd.Key), GetRowIndex(commentToAdd.Key).ToString());
                }

                // The VMLDrawingPart should contain all the definitions for how to draw every comment shape for the worksheet
                VmlDrawingPart vmlDrawingPart = worksheetPart.AddNewPart<VmlDrawingPart>();
                using (XmlTextWriter writer = new XmlTextWriter(vmlDrawingPart.GetStream(FileMode.Create), Encoding.UTF8))
                {

                    writer.WriteRaw("<xml xmlns:v=\"urn:schemas-microsoft-com:vml\"\r\n xmlns:o=\"urn:schemas-microsoft-com:office:office\"\r\n xmlns:x=\"urn:schemas-microsoft-com:office:excel\">\r\n <o:shapelayout v:ext=\"edit\">\r\n  <o:idmap v:ext=\"edit\" data=\"1\"/>\r\n" +
                    "</o:shapelayout><v:shapetype id=\"_x0000_t202\" coordsize=\"21600,21600\" o:spt=\"202\"\r\n  path=\"m,l,21600r21600,l21600,xe\">\r\n  <v:stroke joinstyle=\"miter\"/>\r\n  <v:path gradientshapeok=\"t\" o:connecttype=\"rect\"/>\r\n </v:shapetype>"
                    + commentsVmlXml + "</xml>");
                }

                // Create the comment elements
                foreach (var commentToAdd in commentsToAddDict)
                {
                    WorksheetCommentsPart worksheetCommentsPart = worksheetPart.WorksheetCommentsPart ?? worksheetPart.AddNewPart<WorksheetCommentsPart>();

                    // We only want one legacy drawing element per worksheet for comments
                    if (worksheetPart.Worksheet.Descendants<LegacyDrawing>().SingleOrDefault() == null)
                    {
                        string vmlPartId = worksheetPart.GetIdOfPart(vmlDrawingPart);
                        LegacyDrawing legacyDrawing = new LegacyDrawing() { Id = vmlPartId };
                        worksheetPart.Worksheet.Append(legacyDrawing);
                    }

                    Comments comments;
                    bool appendComments = false;
                    if (worksheetPart.WorksheetCommentsPart.Comments != null)
                    {
                        comments = worksheetPart.WorksheetCommentsPart.Comments;
                    }
                    else
                    {
                        comments = new Comments();
                        appendComments = true;
                    }

                    // We only want one Author element per Comments element
                    if (worksheetPart.WorksheetCommentsPart.Comments == null)
                    {
                        Authors authors = new Authors();
                        Author author = new Author();
                        author.Text = "Expressbase";
                        authors.Append(author);
                        comments.Append(authors);
                    }

                    CommentList commentList;
                    bool appendCommentList = false;
                    if (worksheetPart.WorksheetCommentsPart.Comments != null &&
                        worksheetPart.WorksheetCommentsPart.Comments.Descendants<CommentList>().SingleOrDefault() != null)
                    {
                        commentList = worksheetPart.WorksheetCommentsPart.Comments.Descendants<CommentList>().Single();
                    }
                    else
                    {
                        commentList = new CommentList();
                        appendCommentList = true;
                    }

                    Comment comment = new Comment() { Reference = commentToAdd.Key, AuthorId = (UInt32Value)0U };

                    CommentText commentTextElement = new CommentText();

                    Run run = new Run();

                    RunProperties runProperties = new RunProperties();
                    Bold bold = new Bold();
                    FontSize fontSize = new FontSize() { Val = 8D };
                    Color color = new Color() { Indexed = (UInt32Value)81U };
                    RunFont runFont = new RunFont() { Val = "Tahoma" };
                    RunPropertyCharSet runPropertyCharSet = new RunPropertyCharSet() { Val = 1 };

                    runProperties.Append(bold);
                    runProperties.Append(fontSize);
                    runProperties.Append(color);
                    runProperties.Append(runFont);
                    runProperties.Append(runPropertyCharSet);
                    Text text = new Text();
                    text.Text = commentToAdd.Value;

                    run.Append(runProperties);
                    run.Append(text);

                    commentTextElement.Append(run);
                    comment.Append(commentTextElement);
                    commentList.Append(comment);

                    // Only append the Comment List if this is the first time adding a comment
                    if (appendCommentList)
                    {
                        comments.Append(commentList);
                    }

                    // Only append the Comments if this is the first time adding Comments
                    if (appendComments)
                    {
                        worksheetCommentsPart.Comments = comments;
                    }
                }
            }
        }

        public string GetColumnName(string cellName)
        {
            // Create a regular expression to match the column name portion of the cell name.
            Regex regex = new Regex("[A-Za-z]+");
            Match match = regex.Match(cellName);

            return match.Value;
        }

        public uint GetRowIndex(string cellName)
        {
            // Create a regular expression to match the row index portion the cell name.
            Regex regex = new Regex(@"\d+");
            Match match = regex.Match(cellName);

            return uint.Parse(match.Value);
        }

        /// <summary>
        /// Creates the VML Shape XML for a comment. It determines the positioning of the
        /// comment in the excel document based on the column name and row index.
        /// </summary>
        /// <param name="columnName">Column name containing the comment</param>
        /// <param name="rowIndex">Row index containing the comment</param>
        /// <returns>VML Shape XML for a comment</returns>
        private static string GetCommentVMLShapeXML(string columnName, string rowIndex)
        {
            string commentVmlXml = string.Empty;

            // Parse the row index into an int so we can subtract one
            int commentRowIndex;
            if (int.TryParse(rowIndex, out commentRowIndex))
            {
                commentRowIndex -= 1;

                commentVmlXml = "<v:shape id=\"" + Guid.NewGuid().ToString().Replace("-", "") + "\" type=\"#_x0000_t202\" style=\'position:absolute;\r\n  margin-left:59.25pt;margin-top:1.5pt;width:96pt;height:55.5pt;z-index:1;\r\n  visibility:hidden\' fillcolor=\"#ffffe1\" o:insetmode=\"auto\">\r\n  <v:fill color2=\"#ffffe1\"/>\r\n" +
                "<v:shadow on=\"t\" color=\"black\" obscured=\"t\"/>\r\n  <v:path o:connecttype=\"none\"/>\r\n  <v:textbox style=\'mso-fit-shape-to-text:true'>\r\n   <div style=\'text-align:left\'></div>\r\n  </v:textbox>\r\n  <x:ClientData ObjectType=\"Note\">\r\n   <x:MoveWithCells/>\r\n" +
                "<x:SizeWithCells/>\r\n   <x:Anchor>\r\n" + GetAnchorCoordinatesForVMLCommentShape(columnName, rowIndex) + "</x:Anchor>\r\n   <x:AutoFill>False</x:AutoFill>\r\n   <x:Row>" + commentRowIndex + "</x:Row>\r\n   <x:Column>" + (GetColumnIndex(columnName) - 1) + "</x:Column>\r\n  </x:ClientData>\r\n </v:shape>";
            }

            return commentVmlXml;
        }

        /// <summary>
        /// Gets the coordinates for where on the excel spreadsheet to display the VML comment shape
        /// </summary>
        /// <param name="columnName">Column name of where the comment is located (ie. B)</param>
        /// <param name="rowIndex">Row index of where the comment is located (ie. 2)</param>
        /// <returns><see cref="<x:Anchor>"/> coordinates in the form of a comma separated list</returns>
        private static string GetAnchorCoordinatesForVMLCommentShape(string columnName, string rowIndex)
        {
            string coordinates = string.Empty;
            int startingRow = 1;
            int startingColumn = GetColumnIndex(columnName).Value;

            // From (upper right coordinate of a rectangle)
            // [0] Left column
            // [1] Left column offset
            // [2] Left row
            // [3] Left row offset
            // To (bottom right coordinate of a rectangle)
            // [4] Right column
            // [5] Right column offset
            // [6] Right row
            // [7] Right row offset
            List<int> coordList = new List<int>(8) { 0, 0, 0, 0, 0, 0, 0, 0 };

            if (int.TryParse(rowIndex, out startingRow))
            {
                // Make the row be a zero based index
                startingRow -= 1;

                coordList[0] = startingColumn - 1; // If starting column is A, display shape in column B
                coordList[1] = 15;
                coordList[2] = startingRow;
                coordList[4] = startingColumn + 2; // If starting column is A, display shape till column D
                coordList[5] = 15;
                coordList[6] = startingRow + 3; // If starting row is 0, display 3 rows down to row 3

                // The row offsets change if the shape is defined in the first row
                if (startingRow == 0)
                {
                    coordList[3] = 20;
                    coordList[7] = 16;
                }
                else
                {
                    coordList[3] = 10;
                    coordList[7] = 4;
                }

                coordinates = string.Join(",", coordList.ConvertAll<string>(x => x.ToString()).ToArray());
            }

            return coordinates;
        }

        /// <summary>
        /// Given just the column name (no row index), it will return the zero based column index.
        /// Note: This method will only handle columns with a length of up to two (ie. A to Z and AA to ZZ). 
        /// A length of three can be implemented when needed.
        /// </summary>
        /// <param name="columnName">Column Name (ie. A or AB)</param>
        /// <returns>Zero based index if the conversion was successful; otherwise null</returns>
        private static int? GetColumnIndex(string cellReference)
        {
            if (string.IsNullOrEmpty(cellReference))
            {
                return null;
            }

            //remove digits
            string columnReference = Regex.Replace(cellReference.ToUpper(), @"[\d]", string.Empty);

            int columnNumber = -1;
            int mulitplier = 1;

            //working from the end of the letters take the ASCII code less 64 (so A = 1, B =2...etc)
            //then multiply that number by our multiplier (which starts at 1)
            //multiply our multiplier by 26 as there are 26 letters
            foreach (char c in columnReference.ToCharArray().Reverse())
            {
                columnNumber += mulitplier * ((int)c - 64);

                mulitplier = mulitplier * 26;
            }

            //the result is zero based so return columnnumber + 1 for a 1 based answer
            //this will match Excel's COLUMN function
            return columnNumber + 1;
        }

        private Cell CreateCell(string text)
        {
            Cell cell = new Cell();
            cell.StyleIndex = 0;
            cell.DataType = ResolveCellDataTypeOnValue(text);
            cell.CellValue = new CellValue(text);
            return cell;
        }

        private EnumValue<CellValues> ResolveCellDataTypeOnValue(string text)
        {
            int intVal;
            double doubleVal;
            if (int.TryParse(text, out intVal) || double.TryParse(text, out doubleVal))
            {
                return CellValues.Number;
            }
            else
            {
                return CellValues.String;
            }
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
