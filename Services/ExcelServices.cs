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
            _form.AfterRedisGet(this);

            //List<ColumnsInfo> _cols = new List<ColumnsInfo>();
            //MemoryStream ms = new MemoryStream();
            byte[] bytes = null;
            string _worksheetName = string.IsNullOrEmpty(_form.DisplayName.Trim()) ? _form.Name.Trim() : _form.DisplayName.Trim();
            MemoryStream ms = new MemoryStream();
            var document = SpreadsheetDocument.Create(ms, DocumentFormat.OpenXml.SpreadsheetDocumentType.Workbook);
            WorkbookPart workbookPart = document.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();
            WorksheetPart worksheetPart = workbookPart.AddNewPart<WorksheetPart>("rId1");
            worksheetPart.Worksheet = new Worksheet();
            Sheets sheets1 = new Sheets();
            Sheet sheet1 = new Sheet() { Name = "Sheet1", SheetId = (UInt32Value)1U, Id = document.WorkbookPart.GetIdOfPart(worksheetPart) };
            sheets1.Append(sheet1);
            workbookPart.Workbook.Append(sheets1);
            SheetData sheetData = new SheetData();
            Dictionary<string, string> commentsdict = new Dictionary<string, string>();
            DataValidations newDVs = new DataValidations();


            WorkbookStylesPart stylesheet = workbookPart.AddNewPart<WorkbookStylesPart>();
            stylesheet.Stylesheet = GenerateStylesheet();
            stylesheet.Stylesheet.Save();
            foreach (var _tbl in _form.FormSchema.Tables)
            {
                Row row = new Row();
                int colIndex = 1;
                Columns columns = new Columns();
                string _tblName = _tbl.TableName;
                foreach (var _col in _tbl.Columns)
                {
                    if (_col.Control.ObjType != "ProvisionUser")
                    {
                        columns.Append(new Column() { Min = Convert.ToUInt32(colIndex), Max = Convert.ToUInt32(colIndex), Width = 25, CustomWidth = true });
                        Cell cell = new Cell();
                        cell.StyleIndex = 0;
                        cell.DataType = CellValues.String;
                        if (!string.IsNullOrEmpty(_col.Control.Label))
                            cell.CellValue = new CellValue(_col.Control.Label);
                        else
                            cell.CellValue = new CellValue(_col.Control.Name);
                        string comment = JsonConvert.SerializeObject(new ColumnsInfo { Name = _col.Control.Name, Label = _col.Control.Label, DbType = _col.Control.EbDbType, TableName = _tblName });
                        //workSheet.Cells[1, colIndex].AddComment(comment, "ExpressBase");
                        var cellref = GetExcelColumnName(colIndex) + 1;
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
                        if (_col.Control.EbDbType.ToString() == "Decimal")
                        {
                            cell.StyleIndex = 3;
                            dataValidation.Type = DataValidationValues.Decimal;
                            //cell.DataType = CellValues.Number;
                            dataValidation.Prompt = "Decimal only allowed";
                            dataValidation.PromptTitle = "Enter a integer value here";
                            dataValidation.ErrorTitle = "Invalid Value entered";
                            dataValidation.Error = "This cell must be a valid number.";
                        }

                        else if (_col.Control.EbDbType.ToString() == "Date")
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
                        }

                        else if (_col.Control.EbDbType.ToString() == "DateTime")
                        {
                            cell.StyleIndex = 1;
                            dataValidation.Type = DataValidationValues.Date;
                            //cell.DataType = CellValues.Date;
                            dataValidation.Prompt = "Date only allowed";
                            dataValidation.PromptTitle = "Enter valid date here";
                            dataValidation.ErrorTitle = "Invalid Date  entered";
                            dataValidation.Error = "This cell must be a date ";
                            dataValidation.Operator = DataValidationOperatorValues.GreaterThanOrEqual;
                        }

                        else if (_col.Control.EbDbType.ToString() == "BooleanOriginal")
                        {
                            dataValidation.Type = DataValidationValues.List;
                            string vals = "Yes,No";
                            dataValidation.Formula1 = new Formula1("\"" + vals + "\"");
                        }
                        colIndex++;
                        row.Append(cell);
                        newDVs.Append(dataValidation);
                        newDVs.Count = (newDVs.Count == null) ? 1 : newDVs.Count + 1;
                    }
                }
                if (_tbl.Columns.Count > 0)
                {
                    sheetData.Append(row);
                    worksheetPart.Worksheet.Append(columns);
                    break;
                }
            }
            worksheetPart.Worksheet.Append(sheetData);
            worksheetPart.Worksheet.Append(newDVs);
            InsertComments(worksheetPart, commentsdict);
            worksheetPart.Worksheet.Save();
            workbookPart.Workbook.Save();
            document.Close();
            //workSheet.Cells.AutoFitColumns();
            //workSheet.Calculate();
            //bytes = excel.GetAsByteArray();
            //excel.SaveAs(ms);
            ms.Position = 0;
            bytes = ms.ToArray();
            return new ExcelDownloadResponse { stream = bytes, fileName = _worksheetName + ".xlsx" };
            //return response;
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

        private Cell CreateCell(string text, uint styleIndex)
        {
            Cell cell = new Cell();
            cell.StyleIndex = styleIndex;
            cell.DataType = CellValues.String;
            cell.CellValue = new CellValue(text);
            return cell;
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
