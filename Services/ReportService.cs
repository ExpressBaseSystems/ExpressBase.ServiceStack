
using iTextSharp.text;
using iTextSharp.text.pdf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis.Scripting;
using System.Text.RegularExpressions;
using ServiceStack.Redis;
using ServiceStack.Messaging;
using iTextSharp.text.html.simpleparser;
using ServiceStack;
using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Services;
using ExpressBase.CoreBase.Globals;
using ExpressBase.Common.Singletons;
using System.Diagnostics;
using Newtonsoft.Json;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class ReportService : EbBaseService
    {
        //private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _sfc, _mqp, _mqc) { }

        public MemoryStream Ms1 = null;

        public EbReport Report = null;

        public PdfWriter Writer = null;

        public Document Document = null;

        public PdfContentByte Canvas = null;

        public void GetReportObject(string Refid)
        {
            EbObjectService myObjectservice = base.ResolveService<EbObjectService>();
            myObjectservice.EbConnectionFactory = this.EbConnectionFactory;

            EbObjectParticularVersionResponse resultlist = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = Refid }) as EbObjectParticularVersionResponse;
            Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);
        }

        public void GetData4Pdf(List<Param> _params)
        {
            DataSourceService myDataSourceservice = base.ResolveService<DataSourceService>();
            myDataSourceservice.EbConnectionFactory = this.EbConnectionFactory;
            Report.Parameters = _params;
            if (Report.DataSourceRefId != string.Empty)
            {
                Report.DataSet = myDataSourceservice.Any(new DataSourceDataSetRequest
                {
                    RefId = Report.DataSourceRefId,
                    Params = Report.Parameters,
                    /*Groupings= Groupings*/
                }).DataSet;
            }
            if (Report.DataSet != null)
                FillingCollections(Report);

        }

        public void InitializeReportObects(string BToken, string RToken, string SolnId, string ReadingUserAuthId, string RenderingUserAuthId)
        {
            Report.ReportService = this;
            Report.IsLastpage = false;
            Report.WatermarkImages = new Dictionary<int, byte[]>();
            Report.WaterMarkList = new List<object>();
            Report.ValueScriptCollection = new Dictionary<string, object>();
            Report.AppearanceScriptCollection = new Dictionary<string, object>();
            Report.LinkCollection = new Dictionary<string, List<Common.Objects.EbControl>>();
            Report.GroupSummaryFields = new Dictionary<string, List<EbDataField>>();
            Report.PageSummaryFields = new Dictionary<string, List<EbDataField>>();
            Report.ReportSummaryFields = new Dictionary<string, List<EbDataField>>();
            Report.GroupFooters = new Dictionary<string, ReportGroupItem>();
            Report.Groupheaders = new Dictionary<string, ReportGroupItem>();

            if (string.IsNullOrEmpty(FileClient.BearerToken) && !string.IsNullOrEmpty(BToken))
            {
                FileClient.BearerToken = BToken;
                FileClient.RefreshToken = RToken;
            }
            Report.FileClient = FileClient;

            Report.Solution = GetSolutionObject(SolnId);
            Report.ReadingUser = GetUserObject(ReadingUserAuthId);
            Report.RenderingUser = GetUserObject(RenderingUserAuthId);
            Report.CultureInfo = CultureHelper.GetSerializedCultureInfo(Report.ReadingUser?.Preference.Locale ?? "en-US").GetCultureInfo();

            Report.GetWatermarkImages();
        }

        public void InitializePdfObjects()
        {
            float _width = Report.WidthPt - Report.Margin.Left;// - Report.Margin.Right;
            float _height = Report.HeightPt - Report.Margin.Top - Report.Margin.Bottom;
            Report.HeightPt = _height;

            Rectangle rec = new Rectangle(_width, _height);
            if (this.Document == null)
            {
                Report.Doc = new Document(rec);
                Report.Doc.SetMargins(Report.Margin.Left, Report.Margin.Right, Report.Margin.Top, Report.Margin.Bottom);
                Report.Writer = PdfWriter.GetInstance(Report.Doc, this.Ms1);
                Report.Writer.Open();
                Report.Doc.Open();
                Report.Doc.AddTitle(Report.DocumentName);
                Report.Writer.PageEvent = new HeaderFooter(Report);
                Report.Writer.CloseStream = true;//important
                Report.Canvas = Report.Writer.DirectContent;
                Report.PageNumber = Report.Writer.PageNumber;
                this.Document = Report.Doc;
                this.Writer = Report.Writer;
                this.Canvas = Report.Canvas;
            }
            else
            {
                Report.Doc = this.Document;
                Report.Writer = this.Writer;
                Report.Canvas = this.Canvas;
                Report.PageNumber = 1/*Report.Writer.PageNumber*/;
            }
        }

        public void Draw()
        {
            Report.DrawReportHeader();

            if (Report?.DataSet?.Tables[Report.DetailTableIndex]?.Rows.Count > 0)
            {
                Report.DrawDetail();
            }
            else
            {
                Report.DrawPageHeader();
                Report.detailEnd += 30;
                Report.DrawPageFooter();
                Report.DrawReportFooter();
                throw new Exception("Dataset is null, refid " + Report.DataSourceRefId);
            }

            Report.DrawReportFooter();
        }

        public void HandleExceptionPdf()
        {
            ColumnText ct = new ColumnText(Report.Canvas);
            Phrase phrase;
            if (Report?.DataSet?.Tables[Report.DetailTableIndex]?.Rows.Count > 0)
                phrase = new Phrase("Something went wrong. Please check the parameters or contact admin");
            else
                phrase = new Phrase("No Data available. Please check the parameters or contact admin");

            phrase.Font.Size = 10;
            float y = Report.HeightPt - (Report.ReportHeaderHeight + Report.Margin.Top + Report.PageHeaderHeight);

            ct.SetSimpleColumn(phrase, Report.LeftPt + 30, y - 30, Report.WidthPt - 30, y, 15, Element.ALIGN_CENTER);
            ct.Go();
        }

        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            if (!string.IsNullOrEmpty(request.Refid))
            {
                this.Ms1 = new MemoryStream();
                try
                {
                    GetReportObject(request.Refid);

                    InitializeReportObects(request.BToken, request.RToken, request.SolnId, request.ReadingUserAuthId, request.RenderingUserAuthId);

                    InitializePdfObjects();

                    Report.Doc.NewPage();
                    GetData4Pdf(request.Params);

                    if (Report.DataSet != null)
                        Draw();
                    else
                        throw new Exception();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception-reportService " + e.Message + e.StackTrace);
                    HandleExceptionPdf();
                }

                Report.Doc.Close();

                Report.SetPassword(Ms1);

                string name = Report.DocumentName;

                if (Report.DataSourceRefId != string.Empty && Report.DataSet != null)
                {
                    Report.DataSet.Tables.Clear();
                    Report.DataSet = null;
                }

                this.Ms1.Position = 0;//important

                return new ReportRenderResponse
                {
                    StreamWrapper = new MemorystreamWrapper(this.Ms1),
                    ReportName = name,
                    ReportBytea = this.Ms1.ToArray(),
                    CurrentTimestamp = Report.CurrentTimestamp
                };
            }
            else
            {
                Console.WriteLine("Report render reque reached, but refid is null - " + request.SolnId);
                return null;
            }
        }

        public ReportRenderResponse Get(ReportRenderMultipleRequest request)
        {
            this.Ms1 = new MemoryStream();
            try
            {
                byte[] encodedDataAsBytes = System.Convert.FromBase64String(request.Params);
                string returnValue = System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);

                List<Param> _paramlist = (returnValue == null) ? null : JsonConvert.DeserializeObject<List<Param>>(returnValue);
                if (_paramlist != null)
                {
                    foreach (Param p in _paramlist)
                    {
                        string[] values = p.Value.Split(',');
                        foreach (string val in values)
                        {
                            List<Param> _newParamlist = new List<Param>();
                            _newParamlist.Add(new Param { Name = "id", Value = val, Type = "7" });

                            GetReportObject(request.Refid);

                            InitializePdfObjects();

                            InitializeReportObects(request.BToken, request.RToken, request.SolnId, request.ReadingUserAuthId, request.RenderingUserAuthId);

                            Report.Doc.NewPage();

                            GetData4Pdf(_newParamlist);

                            if (Report.DataSet != null)
                            {
                                Draw();
                            }
                            else throw new Exception();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception-reportService " + e.Message + e.StackTrace);
                HandleExceptionPdf();
            }
            Report.Doc.Close();

            if (Report.DataSourceRefId != string.Empty && Report.DataSet != null)
            {
                Report.DataSet.Tables.Clear();
                Report.DataSet = null;
            }

            Ms1.Position = 0;
            return new ReportRenderResponse
            {
                StreamWrapper = new MemorystreamWrapper(Ms1),
                ReportName = Report.DocumentName,
                ReportBytea = Ms1.ToArray(),
                CurrentTimestamp = Report.CurrentTimestamp
            };

        }

        private Paragraph CreateSimpleHtmlParagraph(String text)
        {

            // Report.Doc.Add(CreateSimpleHtmlParagraph("this is <b>bold</b> text"));
            // Report.Doc.Add(CreateSimpleHtmlParagraph("this is <i>italic</i> text"));
            //Our return object
            Paragraph p = new Paragraph();

            //ParseToList requires a StreamReader instead of just text
            using (StringReader sr = new StringReader(text))
            {
                //Parse and get a collection of elements
                var elements = HtmlWorker.ParseToList(sr, null);
                foreach (IElement e in elements)
                {
                    //Add those elements to the paragraph
                    p.Add(e);
                }
            }
            //Return the paragraph
            return p;
        }

        public void FillingCollections(EbReport Report)
        {
            foreach (EbReportHeader r_header in Report.ReportHeaders)
                Fill(Report, r_header.GetFields(), EbReportSectionType.ReportHeader);

            foreach (EbReportFooter r_footer in Report.ReportFooters)
                Fill(Report, r_footer.GetFields(), EbReportSectionType.ReportFooter);

            foreach (EbPageHeader p_header in Report.PageHeaders)
                Fill(Report, p_header.GetFields(), EbReportSectionType.PageHeader);

            foreach (EbReportDetail detail in Report.Detail)
                Fill(Report, detail.GetFields(), EbReportSectionType.Detail);

            foreach (EbPageFooter p_footer in Report.PageFooters)
                Fill(Report, p_footer.GetFields(), EbReportSectionType.PageFooter);

            foreach (EbReportGroup group in Report.ReportGroups)
            {
                Fill(Report, group.GroupHeader.GetFields(), EbReportSectionType.ReportGroups);
                Fill(Report, group.GroupFooter.GetFields(), EbReportSectionType.ReportGroups);
                foreach (EbReportField field in group.GroupHeader.GetFields())
                {
                    if (field is EbDataField)
                    {
                        Report.Groupheaders.Add((field as EbDataField).ColumnName, new ReportGroupItem
                        {
                            field = field as EbDataField,
                            PreviousValue = string.Empty,
                            order = group.GroupHeader.Order
                        });
                    }
                }
                foreach (EbReportField field in group.GroupFooter.GetFields())
                {
                    if (field is EbDataField)
                    {
                        Report.GroupFooters.Add((field as EbDataField).Name, new ReportGroupItem
                        {
                            field = field as EbDataField,
                            PreviousValue = string.Empty,
                            order = group.GroupHeader.Order
                        });
                    }
                }
            }

        }

        private void Fill(EbReport Report, List<EbReportField> fields, EbReportSectionType section_typ)
        {
            foreach (EbReportField field in fields)
            {
                if (!String.IsNullOrEmpty(field.HideExpression?.Code))
                {
                    if (Report.EvaluatorVersion == EvaluatorVersion.Version_1)
                        Report.ExecuteHideExpressionV1(field);
                    else
                        Report.ExecuteHideExpressionV2(field);
                }
                if (!field.IsHidden && !String.IsNullOrEmpty(field.LayoutExpression?.Code))
                {
                    if (Report.EvaluatorVersion == EvaluatorVersion.Version_1)
                        Report.ExecuteLayoutExpressionV1(field);
                    else
                        Report.ExecuteLayoutExpressionV2(field);
                }
                if (field is EbDataField)
                {
                    EbDataField field_org = field as EbDataField;
                    if (!string.IsNullOrEmpty(field_org.LinkRefId) && !Report.LinkCollection.ContainsKey(field_org.LinkRefId))
                        FindControls(Report, field_org);//Finding the link's parameter controls

                    if (section_typ == EbReportSectionType.Detail)
                        FindLargerDataTable(Report, field_org);// finding the table of highest rowcount from dataset

                    if (field is IEbDataFieldSummary)
                        FillSummaryCollection(Report, field_org, section_typ);

                    if (field is EbCalcField && !Report.ValueScriptCollection.ContainsKey(field.Name) && !string.IsNullOrEmpty((field_org as EbCalcField).ValExpression?.Code))
                    {
                        if (Report.EvaluatorVersion == EvaluatorVersion.Version_1)
                        {
                            Script valscript = Report.CompileScriptV1((field as EbCalcField).ValExpression.Code);
                            Report.ValueScriptCollection.Add(field.Name, valscript);
                        }
                        else
                        {
                            string processedCode = Report.GetProcessedCodeForScriptCollectionV2((field as EbCalcField).ValExpression.Code);
                            Report.ValueScriptCollection.Add(field.Name, processedCode);
                        }
                    }

                    if (!field.IsHidden && !Report.AppearanceScriptCollection.ContainsKey(field.Name) && !string.IsNullOrEmpty(field_org.AppearExpression?.Code))
                    {
                        Script appearscript = Report.CompileScriptV1(field_org.AppearExpression.Code);
                        Report.AppearanceScriptCollection.Add(field.Name, appearscript);
                    }
                }
            }
        }

        public void FindLargerDataTable(EbReport Report, EbDataField field)
        {
            if (!Report.HasRows || field.TableIndex != Report.DetailTableIndex)
            {
                if (Report.DataSet?.Tables.Count > 0)
                {
                    if (Report.DataSet.Tables[field.TableIndex].Rows != null)
                    {
                        Report.HasRows = true;
                        int r_count = Report.DataSet.Tables[field.TableIndex].Rows.Count;
                        Report.DetailTableIndex = (r_count > Report.MaxRowCount) ? field.TableIndex : Report.DetailTableIndex;
                        Report.MaxRowCount = (r_count > Report.MaxRowCount) ? r_count : Report.MaxRowCount;
                    }
                    else
                    {
                        Console.WriteLine("Report.DataSet.Tables[field.TableIndex].Rows is null");
                    }
                }
                else
                {
                    Console.WriteLine("Report.DataSet.Tables.Count is 0");
                }
            }
        }

        public void FindControls(EbReport report, EbDataField field)
        {
            EbObjectService myObjectservice = base.ResolveService<EbObjectService>();
            myObjectservice.EbConnectionFactory = this.EbConnectionFactory;

            string LinkRefid = field.LinkRefId;
            string linkDsRefid = string.Empty;

            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = LinkRefid });
            if (res.Data[0].EbObjectType == 3)
                linkDsRefid = EbSerializers.Json_Deserialize<EbReport>(res.Data[0].Json).DataSourceRefId;//Getting the linked report
            else if (res.Data[0].EbObjectType == 16)
                linkDsRefid = EbSerializers.Json_Deserialize<EbTableVisualization>(res.Data[0].Json).DataSourceRefId;//Getting the linked table viz
            else if (res.Data[0].EbObjectType == 17)
                linkDsRefid = EbSerializers.Json_Deserialize<EbChartVisualization>(res.Data[0].Json).DataSourceRefId;//Getting the linked chart viz

            EbDataReader LinkDatasource = Redis.Get<EbDataReader>(linkDsRefid);
            if (LinkDatasource == null || LinkDatasource.Sql == null || LinkDatasource.Sql == string.Empty)
            {
                EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = linkDsRefid });
                LinkDatasource = EbSerializers.Json_Deserialize(result.Data[1].Json);
                Redis.Set<EbDataReader>(linkDsRefid, LinkDatasource);
            }

            if (!string.IsNullOrEmpty(LinkDatasource.FilterDialogRefId))
            {
                LinkDatasource.FilterDialog = Redis.Get<EbFilterDialog>(LinkDatasource.FilterDialogRefId);
                if (LinkDatasource.FilterDialog == null)
                {
                    EbObjectParticularVersionResponse result = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = LinkDatasource.FilterDialogRefId });
                    LinkDatasource.FilterDialog = EbSerializers.Json_Deserialize(result.Data[1].Json);
                    Redis.Set<EbFilterDialog>(LinkDatasource.FilterDialogRefId, LinkDatasource.FilterDialog);
                }
                report.LinkCollection[LinkRefid] = LinkDatasource.FilterDialog.Controls;
            }
        }

        public void FillSummaryCollection(EbReport report, EbDataField field, EbReportSectionType section_typ)
        {
            if (section_typ == EbReportSectionType.ReportGroups)
            {
                if (!report.GroupSummaryFields.ContainsKey(field.SummaryOf))
                {
                    report.GroupSummaryFields.Add(field.SummaryOf, new List<EbDataField> { field });
                }
                else
                {
                    report.GroupSummaryFields[field.SummaryOf].Add(field);
                }
            }
            if (section_typ == EbReportSectionType.PageFooter)
            {
                if (!report.PageSummaryFields.ContainsKey(field.SummaryOf))
                {
                    report.PageSummaryFields.Add(field.SummaryOf, new List<EbDataField> { field });
                }
                else
                {
                    report.PageSummaryFields[field.SummaryOf].Add(field);
                }
            }
            if (section_typ == EbReportSectionType.ReportFooter)
            {
                if (!report.ReportSummaryFields.ContainsKey(field.SummaryOf))
                {
                    report.ReportSummaryFields.Add(field.SummaryOf, new List<EbDataField> { field });
                }
                else
                {
                    report.ReportSummaryFields[field.SummaryOf].Add(field);
                }
            }
        }

        public ValidateCalcExpressionResponse Get(ValidateCalcExpressionRequest request)
        {
            Type resultType;
            EbDataReader ds = null;
            bool _isValid = true;
            string _excepMsg = string.Empty;
            int resultType_enum = 0;

            EbObjectService myObjectservice = base.ResolveService<EbObjectService>();
            myObjectservice.EbConnectionFactory = this.EbConnectionFactory;
            DataSourceService myDataSourceservice = base.ResolveService<DataSourceService>();
            myDataSourceservice.EbConnectionFactory = this.EbConnectionFactory;

            DataSourceColumnsResponse cresp = new DataSourceColumnsResponse();
            cresp = Redis.Get<DataSourceColumnsResponse>(string.Format("{0}_columns", request.DataSourceRefId));
            if (cresp == null || cresp.Columns.Count == 0)
            {
                ds = Redis.Get<EbDataReader>(request.DataSourceRefId);
                if (ds == null)
                {
                    EbObjectParticularVersionResponse dsresult = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.DataSourceRefId }) as EbObjectParticularVersionResponse;
                    ds = EbSerializers.Json_Deserialize<EbDataReader>(dsresult.Data[0].Json);
                    Redis.Set(request.DataSourceRefId, ds);
                }
                if (ds.FilterDialogRefId != string.Empty)
                    ds.AfterRedisGet(Redis as RedisClient);
                cresp = myDataSourceservice.Any(new DataSourceColumnsRequest { RefId = request.DataSourceRefId, Params = (ds.FilterDialog != null) ? ds.FilterDialog.GetDefaultParams() : null });
                Redis.Set(string.Format("{0}_columns", request.DataSourceRefId), cresp);
            }


            try
            {
                IEnumerable<string> matches = Regex.Matches(request.ValueExpression, @"T[0-9]{1}.\w+").OfType<Match>().Select(m => m.Groups[0].Value).Distinct();

                string[] _dataFieldsUsed = new string[matches.Count()];
                int i = 0;
                foreach (string match in matches)
                    _dataFieldsUsed[i++] = match;

                EbPdfGlobals globals = new EbPdfGlobals();
                {
                    foreach (string calcfd in _dataFieldsUsed)
                    {
                        dynamic _value = null;
                        string TName = calcfd.Split('.')[0];
                        string fName = calcfd.Split('.')[1];
                        EbDbTypes typ = cresp.Columns[Convert.ToInt32(TName.Replace(@"T", string.Empty))][fName].Type;
                        switch (typ.ToString())
                        {
                            case "Int16":
                                _value = 0;
                                break;
                            case "Int32":
                                _value = 0;
                                break;
                            case "Int64":
                                _value = 0;
                                break;
                            case "Decimal":
                                _value = 0;
                                break;
                            case "Double":
                                _value = 0;
                                break;
                            case "Single":
                                _value = 0;
                                break;
                            case "String":
                                _value = "Eb";
                                break;
                            case "Date":
                                _value = DateTime.MinValue;
                                break;
                            case "Datetime":
                                _value = DateTime.MinValue;
                                break;
                            default:
                                _value = 0;
                                break;
                        }
                        globals[TName].Add(fName, new GNTV { Name = fName, Type = (GlobalDbType)(int)typ, Value = _value as object });
                    }
                    if (request.Parameters != null)
                    {
                        foreach (Param p in request.Parameters)
                        {
                            globals["Params"].Add(p.Name, new GNTV { Name = p.Name, Type = (GlobalDbType)Convert.ToInt32(p.Type), Value = p.Value });
                        }
                    }
                    IEnumerable<string> matches2 = Regex.Matches(request.ValueExpression, @"Calc.\w+").OfType<Match>()
                            .Select(m => m.Groups[0].Value)
                            .Distinct();
                    string[] _calcFieldsUsed = new string[matches2.Count()];
                    int j = 0;
                    foreach (string match in matches2)
                        _calcFieldsUsed[j++] = match.Replace("Calc.", string.Empty);
                    foreach (string calcfd in _calcFieldsUsed)
                    {
                        globals["Calc"].Add(calcfd, new GNTV { Name = calcfd, Type = (GlobalDbType)11, Value = 0 });
                    }
                    EbReport R = new EbReport();
                    resultType = R.ExecuteScriptV1(globals, R.CompileScriptV1(request.ValueExpression))?.GetType();

                    //return expression type
                    switch (resultType.FullName)
                    {
                        case "System.Date":
                            resultType_enum = 5;
                            break;
                        case "System.DateTime":
                            resultType_enum = 6;
                            break;
                        case "System.Decimal":
                            resultType_enum = 7;
                            break;
                        case "System.Double":
                            resultType_enum = 8;
                            break;
                        case "System.Int16":
                            resultType_enum = 10;
                            break;
                        case "System.Int32":
                            resultType_enum = 11;
                            break;
                        case "System.Int64":
                            resultType_enum = 12;
                            break;
                        case "System.Single":
                            resultType_enum = 15;
                            break;
                        case "System.String":
                            resultType_enum = 16;
                            break;
                        default:
                            resultType_enum = 0;
                            break;
                    }
                }
            }
            catch (Exception e)
            {
                _isValid = false;
                _excepMsg = e.Message;
                Console.WriteLine(e.Message);
            }
            return new ValidateCalcExpressionResponse { IsValid = _isValid, Type = resultType_enum, ExceptionMessage = _excepMsg };
        }
    }

    public partial class HeaderFooter : PdfPageEventHelper
    {
        private EbReport Report { get; set; }

        public override void OnStartPage(PdfWriter writer, Document document)
        {
        }

        public override void OnEndPage(PdfWriter writer, Document d)
        {
            //var content = writer.DirectContent;
            //var pageBorderRect = new Rectangle(Report.Doc.PageSize);

            //pageBorderRect.Left += Report.Doc.LeftMargin;
            //pageBorderRect.Right -= Report.Doc.RightMargin;
            //pageBorderRect.Top -= Report.Doc.TopMargin;
            //pageBorderRect.Bottom += Report.Doc.BottomMargin;

            //content.SetColorStroke(BaseColor.Red);
            //content.Rectangle(pageBorderRect.Left, pageBorderRect.Bottom, pageBorderRect.Width, pageBorderRect.Height);
            //content.Stroke();

            if (!Report.FooterDrawn && (Report?.DataSet?.Tables[Report.DetailTableIndex]?.Rows.Count > 0))
            {
                Report.DrawPageHeader();
                Report.DrawPageFooter();
            }
            //if (Report.IsLastpage == true)
            //    Report.DrawReportFooter();
            Report.DrawWaterMark(d, writer);
            Report.SetDetail();
        }

        public HeaderFooter(EbReport _c) : base()
        {
            Report = _c;
        }
    }



    //int count = iTextSharp.text.FontFactory.RegisterDirectory("E:\\ExpressBase.Core\\ExpressBase.Objects\\Fonts\\");
    //using (InstalledFontCollection col = new InstalledFontCollection())
    //{
    //    foreach (FontFamily fa in col.Families)
    //    {
    //        Console.WriteLine(fa.Name);
    //    }
    //}

}
