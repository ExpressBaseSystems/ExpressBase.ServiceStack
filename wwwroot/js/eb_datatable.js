//$.fn.dataTable.Api.register('column().data().sum()', function () {
//    return this.reduce(function (a, b) { return a + b; });
//});

//$.fn.dataTable.Api.register('column().data().average()', function () {
//    var sum = this.reduce(function (a, b) { return a + b; });
//    return sum / this.length;
//});

Array.prototype.max = function () {
    return Math.max.apply(null, this);
};

Array.prototype.min = function () {
    return Math.min.apply(null, this);
};

var gi = 0;

function filter_obj(colu, oper, valu)
{
    this.column = colu;
    this.operator = oper;
    this.value = valu;
}

function call_filter(e, objin)
{
    if (e.keyCode == 13)
        $('#' + $(objin).attr('data-table') + '_tbl').DataTable().ajax.reload();
}

function repopulate_filter_arr(table)
{
    var filter_obj_arr = [];
    $.each($('#' + table + '_tbl').DataTable().columns().header().toArray(), function(i, obj)
    {
        var colum = $(obj).children(0).text(); 
        if (colum !== '')
        {
            var oper = $('#' + table + '_' + colum + '_hdr_sel').text();
            var textid = '#' + table + '_' + colum + '_hdr_txt1';
            var type = $(textid).attr('data-coltyp');
            var val1, val2; 
            if ($('#' + table + '_tbl').DataTable().columns(i).visible()[0]) {
                if (oper !== '' && $(textid).val() !== '') {
                    if (oper === 'B') {
                        val1 = $(textid).val();
                        val2 = $(textid).siblings('input').val();
                    }

                    if (oper === 'B' && val1 !== '' && val2 !== '') {
                        if (type == 'numeric') {
                            filter_obj_arr.push(new filter_obj(colum, ">=", Math.min(val1, val2)));
                            filter_obj_arr.push(new filter_obj(colum, "<=", Math.max(val1, val2)));
                        }
                        else if (type == 'date') {
                            if (val2 > val1) {
                                filter_obj_arr.push(new filter_obj(colum, ">=", val1));
                                filter_obj_arr.push(new filter_obj(colum, "<=", val2));
                            }
                            else {
                                filter_obj_arr.push(new filter_obj(colum, ">=", val2));
                                filter_obj_arr.push(new filter_obj(colum, "<=", val1));
                            }
                        }
                    }
                    else
                        filter_obj_arr.push(new filter_obj(colum, oper, $(textid).val()));
                }
        }
        }
    });
    return filter_obj_arr;
}

function createFilterRowHeader(tableid, eb_filter_controls, scrolly)
{
    var __tr = $("<tr role='row'>");

    for (var i = 0; i < eb_filter_controls.length; i++)
        __tr.append($(eb_filter_controls[i]));
    __tr.append("</tr>");
    var __thead = $('#' + tableid + '_container table:eq(0) thead');
    __thead.append(__tr);

    $('#' + tableid + '_container table:eq(0) thead tr:eq(1)').hide();
}

function createFooter(tableid, eb_footer_controls, scrolly, pos)
{
    $('#'+tableid+'_btntotalpage').show();
    if (pos === 1)
        $('#' + tableid + '_container tfoot tr:eq(' + pos + ')').hide();
    $('#' + tableid + '_container tfoot tr:eq(' + pos + ') th').each(function (idx) {
        $(this).html(eb_footer_controls[idx]);
    } );
}

function showOrHideAggrControl(objbtn, scrolly)
{
    var tableid = $(objbtn).attr('data-table');
    if (scrolly != 0) {
            $('#' + tableid + '_container table:eq(2) tfoot tr:eq(1)').toggle();
    }
    else {
        $('#' + tableid + '_container table:eq(0) tfoot tr:eq(1)').toggle();
    }
}

function showOrHideFilter(objbtn, scrolly)
{
    var tableid = $(objbtn).attr('data-table');
    if ($('#' + tableid + '_container table:eq(0) thead tr:eq(1)').is(':visible'))
        $('#' + tableid + '_container table:eq(0) thead tr:eq(1)').hide();
    else
        $('#' + tableid + '_container table:eq(0) thead tr:eq(1)').show();
    clearFilter(tableid);

    $('#' + tableid + '_tbl').DataTable().columns.adjust();
}

function clearFilter(tableid)
{
    $('.' + tableid + '_htext').each(function (i) {
        $(this).val('');
    });
}

function updateAlSlct(objchk)
{
    var tableid = $(objchk).attr('data-table');
    var CkFlag = true;
    $('#' + tableid + '_container tbody [type=checkbox]').each(function (i) {
        if( !this.checked )
            CkFlag = false;
    });             
    $('#' + tableid + '_container table:eq(0) thead [type=checkbox]').prop('checked', CkFlag);
}

function clickAlSlct(e, objchk)
{
    var tableid = $(objchk).attr('data-table');

    if (objchk.checked)
        $('#' + tableid + '_container tbody [type=checkbox]:not(:checked)').trigger('click');
    else
        $('#' + tableid + '_container tbody [type=checkbox]:checked').trigger('click');

    e.stopPropagation();
}

function summarize2(tableId, eb_agginfo,scrollY)
{
    var api = $('#' + tableId + '_tbl').DataTable();
    
    $.each(eb_agginfo, function (index, agginfo) {
        var p = $('#' + tableId + '_' + agginfo.colname + '_ftr_sel1').text().trim();
        if (scrollY>0)
            var ftrtxt = '.dataTables_scrollFoot #' + tableId + '_' + agginfo.colname + '_ftr_txt1';
        else
            var ftrtxt = '#' + tableId + '_' + agginfo.colname + '_ftr_txt1';
        var col = api.column(agginfo.colname + ':name');

        var summary_val = 0;
        if (p === '∑')
            summary_val = col.data().sum();
        if (p === '∓') {
            summary_val = col.data().average();
        } 
        // IF decimal places SET, round using toFixed
        $(ftrtxt).val((agginfo.deci_val > 0) ? summary_val.toFixed(agginfo.deci_val) : summary_val);
    });
}

function fselect_func(objsel, scrollY) {
    var selValue = $(objsel).text().trim();
    $(objsel).parents('.input-group-btn').find('.dropdown-toggle').html(selValue);
    var table = $(objsel).attr('data-table');
    var colum = $(objsel).attr('data-column');
    var decip = $(objsel).attr('data-decip');

    var api = $('#' + table + '_tbl').DataTable();
    var col = api.column(colum + ':name');
    if (scrollY>0)
        var ftrtxt = '.dataTables_scrollFoot #' + table + '_' + colum + '_ftr_txt1';
    else
        var ftrtxt = '#' + table + '_' + colum + '_ftr_txt1';
    if (selValue === '∑')
        pageTotal = col.data().sum();
    else if (selValue === '∓')
        pageTotal = col.data().average();
    // IF decimal places SET, round using toFixed

    $(ftrtxt).val((decip > 0) ? pageTotal.toFixed(decip) : pageTotal);
}

function colorRow(nRow, aData, iDisplayIndex, iDisplayIndexFull, columns)
{
    $.each(columns, function (i, value)
    {
        var rgb = '';
        var fl = '';

        if (value.columnName === 'sys_row_color')
        {
            rgb = (aData[value.columnIndex]).toString();
            var r = rgb.slice(0, -6);
            r = parseInt(r);
            if (r <= 9)
                fl = '0';
            r = r.toString(16);
            if (fl === '0')
                r = '0' + r;

            var g = rgb.slice(3, -3);
            g = parseInt(g);
            if (g <= 9)
                fl = '0';
            g = g.toString(16);
            if (fl === '0')
                g = '0' + g;
            var b = rgb.slice(6, 9);
            b = parseInt(b);
            if (b <= 9)
                fl = '0';
            b = b.toString(16);
            if (fl === '0')
                b = '0' + b;
            rgb = r + g + b;
            $(nRow).css('background-color', '#' + rgb);
        }

        if (value.columnName === 'sys_cancelled') {
            var tr = aData[value.columnIndex];
            if (tr == true)
                $(nRow).css('color', '#f00');
        }
    });
}

function setLiValue(objli)
{
    var selText = $(objli).text();
    var table = $(objli).attr('data-table');
    var colum = $(objli).attr('data-colum'); 
    $(objli).parents('.input-group-btn').find('.dropdown-toggle').html(selText);
    $(objli).parents('.input-group').find('#' + table + '_' + colum + '_hdr_txt2').eq(0).css('visibility', ((selText.trim() === 'B') ? 'visible' : 'hidden'));
}

function renderProgressCol(data)
{
    return "<div class='progress'><div class='progress-bar' role='progressbar' aria-valuenow='" + data.toString() + "' aria-valuemin='0' aria-valuemax='100' style='width:" + data.toString() + "%'></div></div>";
}

function renderCheckBoxCol(datacolumns, tableid, row)
{
    var idpos = (_.find(datacolumns, { 'columnName': 'id' })).columnIndex;
    return "<input type='checkbox' name='" + tableid + "_id' value='" + row[idpos].toString() + "' data-table='" + tableid + "' onclick='updateAlSlct(this);' />";
}

function renderEbVoidCol()
{
    return "<div class='checkbox'><input type='checkbox' data-toggle='toggle'></div>";
}
//datacolumns, data, meta, colname
function lineGraphDiv(data)
{
    //var idpos = (_.find(datacolumns, { 'columnName': colname })).columnIndex;
    //return "<canvas id='can" + meta.row + "' width='120' height='40' data-graph='" + data[idpos] + "'></canvas><script>renderLineGraphs(" + meta.row + ");</script>";
    return "<canvas id='can" + ++gi + "' style='width:120px; height:40px;' data-graph='" + data + "'></canvas><script>renderLineGraphs(" + gi + ");</script>";
}

function renderLineGraphs(id)
{
    var canvas = document.getElementById('can' + id);
    var gdata = $(canvas).attr("data-graph").toString();
    var context = canvas.getContext('2d');
    if (gdata)
    {
        context.fillStyle = "rgba(255, 255, 255, 1)";
        context.beginPath();
        context.fillRect(0, 0, 1000, 1000);
        context.fillStyle = "rgba(51, 122, 183, 0.7)";
        context.moveTo(8, 1000);
        var Gpoints = [];
        var Ypoints = [];
        Gpoints = gdata.split(",");
        var xPoint = 8;
        var xInterval = (parseInt(canvas.style.width) * 3.6) / (Gpoints.length); 
        var yPoint;
        for (var i = 0; i < Gpoints.length; i++) {
            yPoint = parseInt(Gpoints[i].split(":")[1]); 
            Ypoints.push(yPoint);
        }
        var Ymax = Ypoints.max();
        for (var i = 0; i < Gpoints.length; i++) {
            yPoint = parseInt(Gpoints[i].split(":")[1]); 
            context.lineTo(xPoint, 3.76 * (40 - ((yPoint / Ymax) * 40)) );//
            xPoint += xInterval;
        }
        context.lineTo(xPoint - xInterval, 1000);
        canvas.strokeStyle = "black";
        context.fill();
        context.stroke();
    }
  
}

