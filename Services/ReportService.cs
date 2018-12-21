using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Objects;
using ExpressBase.Objects.ReportRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using iTextSharp.text;
using iTextSharp.text.pdf;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using QRCoder;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using System.Dynamic;
using System.Text;
using ExpressBase.ServiceStack.Services;
using ExpressBase.Security;
using System.DrawingCore.Text;
using System.DrawingCore;
using ExpressBase.Common.ServiceClients;
using System.Text.RegularExpressions;
using ServiceStack.Redis;
using ExpressBase.Common.Structures;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Objects;
using ExpressBase.Objects.Objects;
using System.Net.Mail;
using System.Net;
using ServiceStack.Messaging;
using System.Globalization;

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        //private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _sfc, _mqp, _mqc) { }

        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            { //int count = iTextSharp.text.FontFactory.RegisterDirectory("E:\\ExpressBase.Core\\ExpressBase.Objects\\Fonts\\");
              //using (InstalledFontCollection col = new InstalledFontCollection())
              //{
              //    foreach (FontFamily fa in col.Families)
              //    {
              //        Console.WriteLine(fa.Name);
              //    }
              //}
            }
            EbReport Report = null;
            try
            {
                EbObjectService myObjectservice = base.ResolveService<EbObjectService>();
                myObjectservice.EbConnectionFactory = this.EbConnectionFactory;
                DataSourceService myDataSourceservice = base.ResolveService<DataSourceService>();
                myDataSourceservice.EbConnectionFactory = this.EbConnectionFactory;

                EbObjectParticularVersionResponse resultlist = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.Refid }) as EbObjectParticularVersionResponse;
                Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);

                Report.ReportService = this;
                Report.SolutionId = request.SolnId;
                Report.IsLastpage = false;
                Report.WatermarkImages = new Dictionary<int, byte[]>();
                Report.WaterMarkList = new List<object>();
                Report.ValueScriptCollection = new Dictionary<string, Script>();
                Report.AppearanceScriptCollection = new Dictionary<string, Script>();
                Report.LinkCollection = new Dictionary<string, List<Common.Objects.EbControl>>();
                Report.PageSummaryFields = new Dictionary<string, List<EbDataField>>();
                Report.ReportSummaryFields = new Dictionary<string, List<EbDataField>>();
                Report.FileClient = new EbStaticFileClient();
                Report.FileClient = FileClient;
                Report.Solution = Redis.Get<Eb_Solution>(String.Format("solution_{0}", request.SolnId));
                Report.ReadingUser = request.ReadingUser;
                Report.RenderingUser = request.RenderingUser;
                Report.CultureInfo = CultureInfo.GetCultureInfo(Report.ReadingUser.Preference.Locale);
                Report.Parameters = request.Params;
                //-- END REPORT object INIT
                iTextSharp.text.Rectangle rec = new iTextSharp.text.Rectangle(Report.WidthPt, Report.HeightPt);
                Report.Doc = new Document(rec);
                Report.Ms1 = new MemoryStream();
                if (Report.DataSourceRefId != string.Empty)
                    Report.DataSet = myDataSourceservice.Any(new DataSourceDataSetRequest { RefId = Report.DataSourceRefId, Params = Report.Parameters }).DataSet;
                Report.Writer = PdfWriter.GetInstance(Report.Doc, Report.Ms1);
                Report.Writer.Open();
                Report.Doc.Open();
                Report.Doc.AddTitle(Report.Name);
                Report.Writer.PageEvent = new HeaderFooter(Report);
                Report.Writer.CloseStream = true;//important
                Report.Canvas = Report.Writer.DirectContent;
                Report.PageNumber = Report.Writer.PageNumber;
                Report.GetWatermarkImages();
                FillingCollections(Report);

                Report.Doc.NewPage();
                Report.DrawReportHeader();
                Report.DrawDetail();
                Report.Doc.Close();
                if (Report.UserPassword != string.Empty || Report.OwnerPassword != string.Empty)
                    Report.SetPassword();
                Report.Ms1.Position = 0;//important
                if (Report.DataSourceRefId != string.Empty)
                {
                    Report.DataSet.Tables.Clear();
                    Report.DataSet = null;
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Exception-reportService " + e.ToString());
                Console.ForegroundColor = ConsoleColor.White;
            }
            return new ReportRenderResponse
            {
                StreamWrapper = new MemorystreamWrapper(Report.Ms1),
                ReportName = Report.DisplayName,
                ReportBytea = Report.Ms1.ToArray(),
                CurrentTimestamp = Report.CurrentTimestamp
            };
        }
        public void FillingCollections(EbReport Report)
        {
            foreach (EbReportHeader r_header in Report.ReportHeaders)
                Fill(Report, r_header.Fields, EbReportSectionType.ReportHeader);

            foreach (EbReportFooter r_footer in Report.ReportFooters)
                Fill(Report, r_footer.Fields, EbReportSectionType.ReportFooter);

            foreach (EbPageHeader p_header in Report.PageHeaders)
                Fill(Report, p_header.Fields, EbReportSectionType.PageHeader);

            foreach (EbReportDetail detail in Report.Detail)
                Fill(Report, detail.Fields, EbReportSectionType.Detail);

            foreach (EbPageFooter p_footer in Report.PageFooters)
                Fill(Report, p_footer.Fields, EbReportSectionType.ReportFooter);
        }

        private void Fill(EbReport Report, List<EbReportField> fields, EbReportSectionType section_typ)
        {
            foreach (EbReportField field in fields)
            {
                if (field is EbDataField)
                {
                    EbDataField field_org = field as EbDataField;
                    if (!string.IsNullOrEmpty(field_org.LinkRefId) && !Report.LinkCollection.ContainsKey(field_org.LinkRefId))
                        FindControls(Report, field_org);//Finding the link's parameter controls

                    if (section_typ == EbReportSectionType.Detail)
                        FindLargerDataTable(Report, field_org);// finding the table of highest rowcount from dataset

                    if (field is IEbDataFieldSummary)
                        FillSummaryCollection(Report, field_org, section_typ);

                    if (field is EbCalcField && !Report.ValueScriptCollection.ContainsKey(field.Name))
                    {
                        Script valscript = CSharpScript.Create<dynamic>((field as EbCalcField).ValueExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq"), globalsType: typeof(Globals));
                        valscript.Compile();
                        Report.ValueScriptCollection.Add(field.Name, valscript);
                    }

                    if (!Report.AppearanceScriptCollection.ContainsKey(field.Name) && field_org.AppearanceExpression != "")
                    {
                        Script appearscript = CSharpScript.Create<dynamic>(field_org.AppearanceExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq"), globalsType: typeof(Globals));
                        appearscript.Compile();
                        Report.AppearanceScriptCollection.Add(field.Name, appearscript);
                    }
                }
            }
        }
        public void FindLargerDataTable(EbReport Report, EbDataField field)
        {
            Report.HasRows = true;
            int r_count = Report.DataSet.Tables[field.TableIndex].Rows.Count;
            Report.DetailTableIndex = (r_count > Report.MaxRowCount) ? field.TableIndex : Report.DetailTableIndex;
            Report.MaxRowCount = (r_count > Report.MaxRowCount) ? r_count : Report.MaxRowCount;
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
            Script valscript = CSharpScript.Create<dynamic>(request.ValueExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic", "System.Diagnostics", "System.Linq"), globalsType: typeof(Globals));

            try
            {
                valscript.Compile();

                IEnumerable<string> matches = Regex.Matches(request.ValueExpression, @"T[0-9]{1}.\w+").OfType<Match>().Select(m => m.Groups[0].Value).Distinct();

                string[] _dataFieldsUsed = new string[matches.Count()];
                int i = 0;
                foreach (string match in matches)
                    _dataFieldsUsed[i++] = match;

                Globals globals = new Globals();
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
                        globals[TName].Add(fName, new NTV { Name = fName, Type = typ, Value = _value as object });
                    }
                    if (request.Parameters != null)
                    {
                        foreach (Param p in request.Parameters)
                        {
                            globals["Params"].Add(p.Name, new NTV { Name = p.Name, Type = (EbDbTypes)Convert.ToInt32(p.Type), Value = p.Value });
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
                        globals["Calc"].Add(calcfd, new NTV { Name = calcfd, Type = (EbDbTypes)11, Value = 0 });
                    }

                    resultType = (valscript.RunAsync(globals)).Result.ReturnValue.GetType();

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
            Report.DrawPageHeader();
            Report.DrawPageFooter();
            if (Report.IsLastpage == true)
                Report.DrawReportFooter();
            Report.DrawWaterMark(d, writer);
            Report.SetDetail();
        }

        public HeaderFooter(EbReport _c) : base()
        {
            Report = _c;
        }
    }
}
