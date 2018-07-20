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

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        private DataSourceDataSetResponse dsresp = null;

        //private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc) : base(_dbf, _sfc) { }
        
        public ReportRenderResponse Get(ReportRenderRequest request)
        { { //int count = iTextSharp.text.FontFactory.RegisterDirectory("E:\\ExpressBase.Core\\ExpressBase.Objects\\Fonts\\");
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
                var myObjectservice = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse resultlist = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.Refid }) as EbObjectParticularVersionResponse;
                Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);
                Report.ReportService = this;
                Report.SolutionId = request.TenantAccountId;
                Report.IsLastpage = false;
                Report.WatermarkImages = new Dictionary<string, byte[]>();
                Report.WaterMarkList = new List<object>();
                Report.ValueScriptCollection = new Dictionary<string, Script>();
                Report.AppearanceScriptCollection = new Dictionary<string, Script>();
               // Report.FieldDict = new Dictionary<string, object>();
                Report.LinkCollection = new Dictionary<string, List<Common.Objects.EbControl>>();
                Report.CurrentTimestamp = DateTime.Now;
                Report.UserName = request.Fullname;
                Report.FileClient = new EbStaticFileClient();
                Report.FileClient = this.FileClient;
                Report.Parameters = request.Params;
                //var x = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", request.TenantAccountId));
                //-- END REPORT object INIT

                iTextSharp.text.Rectangle rec = new iTextSharp.text.Rectangle(Report.WidthPt, Report.HeightPt);
                Report.Doc = new Document(rec);
                Report.Ms1 = new MemoryStream();
                var myDataSourceservice = base.ResolveService<DataSourceService>();
                if (Report.DataSourceRefId != string.Empty)
                {
                    dsresp = myDataSourceservice.Any(new DataSourceDataSetRequest { RefId = Report.DataSourceRefId, Params = Report.Parameters });
                    Report.DataSet = dsresp.DataSet;
                }

                Report.Writer = PdfWriter.GetInstance(Report.Doc, Report.Ms1);
                Report.Writer.Open();
                Report.Doc.Open();
                Report.Doc.AddTitle(Report.Name);
                Report.Writer.PageEvent = new HeaderFooter(Report);
                Report.Writer.CloseStream = true;//important
                Report.Canvas = Report.Writer.DirectContent;
                Report.PageNumber = Report.Writer.PageNumber;
                Report.InitializeSummaryFields();

                Report.GetWatermarkImages();
                Report.FillingCollections();
                foreach (EbReportHeader r_header in Report.ReportHeaders)
                {
                    //Report.FillScriptCollection(r_header.Fields);
                    //FillFieldDict(Report, r_header.Fields);
                    FillLinkCollection(Report, r_header.Fields);
                }

                foreach (EbReportFooter r_footer in Report.ReportFooters)
                {
                    //FillScriptCollection(Report, r_footer.Fields);
                    //FillFieldDict(Report, r_footer.Fields);
                    FillLinkCollection(Report, r_footer.Fields);
                }

                foreach (EbPageHeader p_header in Report.PageHeaders)
                {
                    //FillScriptCollection(Report, p_header.Fields);
                    //FillFieldDict(Report, p_header.Fields);
                    FillLinkCollection(Report, p_header.Fields);
                }

                foreach (EbReportDetail detail in Report.Detail)
                {
                    //FillScriptCollection(Report, detail.Fields);
                    //FillFieldDict(Report, detail.Fields);
                    FillLinkCollection(Report, detail.Fields);
                }

                foreach (EbPageFooter p_footer in Report.PageFooters)
                {
                    //FillScriptCollection(Report, p_footer.Fields);
                    //FillFieldDict(Report, p_footer.Fields);
                    FillLinkCollection(Report, p_footer.Fields);
                }

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
                Console.WriteLine("Exception-reportService " + e.ToString());
            }
            return new ReportRenderResponse { StreamWrapper = new MemorystreamWrapper(Report.Ms1), ReportName = Report.Name };
        }

        //private void FillScriptCollection(EbReport Report, List<EbReportField> fields)
        //{
        //    foreach (EbReportField field in fields)
        //    {
        //        try
        //        {
        //            if (field is EbCalcField && !Report.ValueScriptCollection.ContainsKey(field.Name))
        //            {
        //                Script valscript = CSharpScript.Create<dynamic>((field as EbCalcField).ValueExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
        //                valscript.Compile();
        //                Report.ValueScriptCollection.Add(field.Name, valscript);

        //            }
        //            if ((field is EbDataField && !Report.AppearanceScriptCollection.ContainsKey(field.Name) && (field as EbDataField).AppearanceExpression != ""))
        //            {
        //                Script appearscript = CSharpScript.Create<dynamic>((field as EbDataField).AppearanceExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
        //                appearscript.Compile();
        //                Report.AppearanceScriptCollection.Add(field.Name, appearscript);
        //            }
        //        }
        //        catch (Exception e)
        //        {
        //            Console.WriteLine(e.Message + e.StackTrace);
        //        }
        //    }
        //}

        //private void FillFieldDict(EbReport Report, List<EbReportField> fields)
        //{
        //    foreach (EbReportField field in fields)
        //    {
        //        Report.FieldDict.Add(field.Name, field);
        //    }
        //}

        private void FillLinkCollection(EbReport Report, List<EbReportField> fields)
        {
            foreach (EbReportField field in fields)
            {
                if (field is EbDataField)
                {
                    string LinkRefid = (field as EbDataField).LinkRefid;
                    if (!string.IsNullOrEmpty((field as EbDataField).LinkRefid))
                    {
                        var myObjectservice = base.ResolveService<EbObjectService>();
                        var res = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = LinkRefid });
                        EbReport linkreport = EbSerializers.Json_Deserialize<EbReport>(res.Data[0].Json);
                        try
                        {
                            EbDataSource LinkDatasource = Redis.Get<EbDataSource>(linkreport.DataSourceRefId);
                            if (LinkDatasource == null || LinkDatasource.Sql == null || LinkDatasource.Sql == string.Empty)
                            {
                                var result = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = linkreport.DataSourceRefId });
                                LinkDatasource = EbSerializers.Json_Deserialize(result.Data[1].Json);
                                Redis.Set<EbDataSource>(linkreport.DataSourceRefId, LinkDatasource);
                            }

                            if (!string.IsNullOrEmpty(LinkDatasource.FilterDialogRefId))
                            {
                                LinkDatasource.FilterDialog = Redis.Get<EbFilterDialog>(LinkDatasource.FilterDialogRefId);
                                if (LinkDatasource.FilterDialog == null)
                                {
                                    var result = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = LinkDatasource.FilterDialogRefId });
                                    LinkDatasource.FilterDialog = EbSerializers.Json_Deserialize(result.Data[1].Json);
                                    Redis.Set<EbFilterDialog>(LinkDatasource.FilterDialogRefId, LinkDatasource.FilterDialog);
                                }
                                Report.LinkCollection[LinkRefid] = LinkDatasource.FilterDialog.Controls;
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Exception: " + e.ToString());
                        }

                    }
                }
            }
        }

        public ValidateCalcExpressionResponse Get(ValidateCalcExpressionRequest request)
        {
            Type resultType;
            EbDataSource ds = null;
            bool _isValid = true;
            string _excepMsg = string.Empty;
            int resultType_enum = 0;
            var myObjectservice = base.ResolveService<EbObjectService>();
            var myDataSourceservice = base.ResolveService<DataSourceService>();
            DataSourceColumnsResponse cresp = new DataSourceColumnsResponse();
            cresp = Redis.Get<DataSourceColumnsResponse>(string.Format("{0}_columns", request.DataSourceRefId));
            if (cresp == null || cresp.Columns.Count == 0)
            {
                ds = Redis.Get<EbDataSource>(request.DataSourceRefId);
                if (ds == null)
                {
                    EbObjectParticularVersionResponse dsresult = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.DataSourceRefId }) as EbObjectParticularVersionResponse;
                    ds = EbSerializers.Json_Deserialize<EbDataSource>(dsresult.Data[0].Json);
                    Redis.Set(request.DataSourceRefId, ds);
                }
                if (ds.FilterDialogRefId != string.Empty)
                    ds.AfterRedisGet(Redis as RedisClient);
                cresp = myDataSourceservice.Any(new DataSourceColumnsRequest { RefId = request.DataSourceRefId, Params = (ds.FilterDialog != null) ? ds.FilterDialog.GetDefaultParams() : null });
                Redis.Set(string.Format("{0}_columns", request.DataSourceRefId), cresp);
            }
            Script valscript = CSharpScript.Create<dynamic>(request.ValueExpression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));

            try
            {
                valscript.Compile();
          
            var matches = Regex.Matches(request.ValueExpression, @"T[0-9]{1}.\w+").OfType<Match>().Select(m => m.Groups[0].Value).Distinct();
            string[] _dataFieldsUsed = new string[matches.Count()];
            int i = 0;
            foreach (var match in matches)
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
                var matches2 = Regex.Matches(request.ValueExpression, @"Calc.\w+").OfType<Match>()
                        .Select(m => m.Groups[0].Value)
                        .Distinct();
                string[] _calcFieldsUsed = new string[matches2.Count()];
                int j = 0;
                foreach (var match in matches2)
                    _calcFieldsUsed[j++] = match.Replace("Calc.",string.Empty);
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
            this.Report = _c;
        }
    }
}
