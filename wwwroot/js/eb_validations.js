function isRequired(element) {
    var ele = document.getElementById(element.id);
    if (ele.value.trim() === '')
        ele.setCustomValidity("This field is required");
    else
        ele.setCustomValidity("");
}

function isUnique(element) {
    var tableid = document.getElementById("tableid").value;
    var ele = document.getElementById(element.id);

    if (ele.value.trim().length > 0) {
        var dict = "{" + element.id.toString() + ":" + ele.value.trim()  + "}";

        $.post('http://localhost:53125/uc', { "TableId": tableid, "Colvalues": dict },
        function (result) {
            if (result) {
                $(element).next().html("<img src='http://localhost:53125/images/CheckMark-24x32.png' width='22px'/>");
            }
            else {
                $(element).next().html("<img src='http://localhost:53125/images/Error-24x24.png' width='22px'/>");
            }
        });
    }
}

function textTransform(element, transform_type) {
    setTimeout(function () {
        if (transform_type === 1)
            $(element).val($(element).val().trim().toLowerCase());
        else if (transform_type === 2)
            $(element).val($(element).val().trim().toUpperCase());
    }, 100);
}

function ZmaxLen(el, len, noDec, evt) {
    var pval = $(el).val();
    var result;
    setTimeout(function () {
        if (!(evt.keyCode == 86)) {
            if ((pval + "").includes(".") && (evt.keyCode == 110))
                result = pval;
            else {
                var txt = $(el).val() + "";
                if ((txt + "").includes("."))
                    result = txt.slice(0, len + 1);
                else
                    result = txt.slice(0, len);
            }
            $(el).val(getNumsFrom(result));
        }
    }, 1);
}

function ZnumValidate(el, l, ndp) {
    setTimeout(function () {
        var txt = getNumsFrom($(el).val() + "");
        if (((txt.match(/[.]/g) || []).length) > 1)
            txt = "";
        if (txt !== "" && (txt.length > l + 1))
            txt = roundUpTo(txt, l);
        txt = fixZeros(txt, ndp, l)
        $(el).val(txt);
    }, 1);
}

function getNumsFrom(str) {
    return str.replace(/[^0-9\.]/g, '');
}

function roundUpTo(val, l) {
    if (val.includes("."))
        l = l + 1;
    return val.slice(0, l - 1) + (parseInt(val[l - 1]) + Math.round(parseInt(val[l]) / 10));
}

function fixZeros(val, ndp, l) {
    if (val.length < l && !val.includes("."))
        val = val + ".";
    if (val.includes(".")) {
        l = l + 1;
        var pi = val.indexOf(".");
        for (i = pi + 1; i < l; i++) {
            if (val[i] == undefined)
                val += "0";
            if (i == pi + ndp)
                break;
        }
        return val.slice(0, l - 1);
    }
    else
        return val.slice(0, l - 1);
}
function submitbutton() {

    var form = $("#Form_0").serialize();
    var data1 = query_to_hash(form);
    var tableid = document.getElementById("tableid").value;

    $.post('http://localhost:53125/insert', { "TableId": tableid, "Colvalues": data1 },

       function (result) {
          
           if (result) {
               window.location.href = "http://localhost:53125/sample/masterhome.cshtml";
           }
           else {         
               auto_load("/sample/f", "#Form_0");
           }
       });
    

}

function signinbutton()
{
    var form = $("#Form_0").serialize();
   
    var data1 = query_to_hash(form);
    var tableid = document.getElementById("tableid").value;

    $.post('http://localhost:53125/login', { "TableId": tableid, "Colvalues": data1 },

       function (result) {

           if (result) {
               window.location.href = "http://localhost:53125/sample/masterhome.cshtml";
           }
           else {
               auto_load("/sample/f", "#Form_0");
           }
       });
}
query_to_hash = function (queryString) {
    var j, q,t = "";
   
    q = (unescape(queryString).replace(/\?/, "").split("&"));

    j = {};
    $.each(q, function (i, arr) {
        arr = arr.replace('=', ':');
        
            var aRR = arr.split(":");
            arr =  aRR[0] +":"+"\""+aRR[1]+"\"";
        
        t += arr+",";
    });
    t += "";
    return t;
}


function deletebutton()
{
    var tableid = document.getElementById("tableid").value;
    var form = $("#Form_0").serialize();
    var dataid = document.getElementById("dataid").value;
    var isupdate = document.getElementById("isUpdate").value;
    var fid = document.getElementById("FId").value;
    var modal = document.getElementById("myModal");
  //  var btn = document.getElementById("confirm");
   
    modal.style.display ="block";
 
    window.onclick = function (event) {
        if (event.target == modal) {
            modal.style.display = "block";
        }
    }
    document.getElementById('confirm').onclick = function () {
        var DataCollection = { 'eb_del': 'true', 'DataId': dataid, "isUpdate": isupdate, "FId": fid, "TableId": tableid };
        $.post('http://localhost:53125/insert', { "TableId": tableid, "Colvalues": JSON.stringify(DataCollection) },

           function (result) {
               if (result) {
                   window.location.href = "http://localhost:53125/sample/masterhome.cshtml";
               }
               else {
                   auto_load("/sample/f", "#Form_0");
               }
           });
    };
    document.getElementById('cancel').onclick = function () {
        modal.style.display = "none";
    }
}
function auto_load(url, form) {
    $.ajax({
        url: url,
        cache: false,
        success: function (data) {
            $(form).html(data);
        }
    });
}

