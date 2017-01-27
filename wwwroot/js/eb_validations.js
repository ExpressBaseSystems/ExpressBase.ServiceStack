function isRequired(element) {
    var ele = document.getElementById(element.id);
    if (ele.value.trim() === '')
        ele.setCustomValidity("This field is required");
    else
        ele.setCustomValidity("");
}

function isUnique(element) {
    var ele = document.getElementById(element.id);

    if (ele.value.trim().length > 0) {
        var dict = "{" + element.id.toString() + ":" + ele.value.trim()  + "}";

        $.post('http://localhost:53125/uc', { "TableId": 157, "Colvalues": dict },
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
    }, 1);
}

