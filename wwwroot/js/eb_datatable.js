//$.fn.dataTable.Api.register('column().data().sum()', function () {
//    return this.reduce(function (a, b) { return a + b; });
//});

//$.fn.dataTable.Api.register('column().data().average()', function () {
//    var sum = this.reduce(function (a, b) { return a + b; });
//    return sum / this.length;
//});

function filter_obj(colu, oper, valu)
{
    this.column = colu;
    this.operator = oper;
    this.value = valu;
}

function fselect_func(objsel)
{
    var table = $(objsel).attr('data-table');
    var colum = $(objsel).attr('data-column');
    var decip = $(objsel).attr('data-decip');

    var api = $('#' + table + '_tbl').DataTable();
    var col = api.column(colum + ':name');

    var ftrtxt = table + '_' + colum + '_ftr_txt1';

    if (objsel.value === 'Sum')
        pageTotal = col.data().sum();
    else if (objsel.value === 'Avg')
        pageTotal = col.data().average();

    // IF decimal places SET, round using toFixed
    $('#' + ftrtxt).val((decip > 0) ? pageTotal.toFixed(decip) : pageTotal);
}

function call_filter_selchange(e, objsel)
{
    if ($(objsel).val() == 'B')
        $(objsel).next().next().css('visibility', 'visible');
    else
        $(objsel).next().next().css('visibility', 'hidden');
}

function call_filter(e, objin)
{
    if (e.keyCode == 13)
        $('#' + $(objin).attr('data-table') + '_tbl').DataTable().ajax.reload();
}

function repopulate_filter_arr(table)
{
    var filter_obj_arr = [];
    $('.' + table + '_htext').each(function (i, obj)
    {
        if ($(this).val() !== '')
        {
            var tid = $(this).attr("id");
            var colum = $(this).attr('data-colum');
            var oper = $('#' + table + '_' + colum + "_hdr_sel").val();

            var _okflag = (tid.endsWith("_txt1")) ? true : false;
            var val1 = parseInt($(this).val());
            var val2 = parseInt((_okflag === true) ? $(this).next().val() : $(this).prev().val());

            if (oper === 'B' && val1 !== '' && val2 !== '')
            {
                filter_obj_arr.push(new filter_obj(colum, ">=", Math.min(val1, val2)));
                filter_obj_arr.push(new filter_obj(colum, "<=", Math.max(val1, val2)));
            }
            else
                filter_obj_arr.push(new filter_obj(colum, oper, $(this).val()));
        }
    });
    return filter_obj_arr;
}

function createFilterRowHeader(tableid, eb_filter_controls, scrolly)
{
    $('#' + tableid + '_container thead').append($('#' + tableid + '_container thead tr').clone());
    $('#' + tableid + '_container thead tr:eq(1)').hide();
    $('#' + tableid + '_container thead tr:eq(1) th').each(function (idx) { $(this).children().remove(); $(this).removeClass('sorting'); $(this).html(eb_filter_controls[idx]); });
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
    if ($('#' + tableid + '_container table:eq(2) tfoot tr:eq(1)').is(':visible'))
        $('#' + tableid + '_container table:eq(2) tfoot tr:eq(1)').hide();
    else
        $('#' + tableid + '_container table:eq(2) tfoot tr:eq(1)').show();
}

function showOrHideFilter(objbtn, scrolly)
{
    var tableid = $(objbtn).attr('data-table');
    if ($('#' + tableid + '_container thead tr:eq(1)').is(':visible'))
        $('#' + tableid + '_container thead tr:eq(1)').hide();
    else
        $('#' + tableid + '_container thead tr:eq(1)').show();
    $('#' + tableid + '_tbl').DataTable().columns.adjust();
}

function updateAlSlct(objchk)
{
    var tableid = $(objchk).attr('data-table');
    var CkFlag = true;
    $('#' + tableid + '_container tbody [type=checkbox]').each(function (i) {
        if( !this.checked )
            CkFlag = false;
    });             
    $('#' + tableid + '_container thead [type=checkbox]').prop('checked', CkFlag);
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

function summarize2(nRow, aaData, iStart, iEnd, aiDisplay, tableId, eb_agginfo)
{
    var api = $('#' + tableId + '_tbl').DataTable();

    $.each(eb_agginfo, function (index, agginfo) {
        var p = $('#' + tableId + '_' + agginfo.colname + '_ftr_sel1').val();
        var ftrtxt = '#' + tableId + '_' + agginfo.colname + '_ftr_txt1';
        var col = api.column(agginfo.colname + ':name');

        var summary_val = 0;
        if (p === 'Sum')
            summary_val = col.data().sum();
        if (p === 'Avg') {
            summary_val = col.data().average();
        }

        // IF decimal places SET, round using toFixed
        $(ftrtxt).val((agginfo.deci_val > 0) ? summary_val.toFixed(agginfo.deci_val) : summary_val);
    });
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


