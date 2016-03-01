var setting = {
    async: {
        enable: true,
        url: getUrl,
        autoParam: [ "id", "name" ],
        otherParam: {},
        dataFilter: filter
    },
    view: {
        expandSpeed: "fast",
        selectedMulti: false
    },
    data: {
        simpleData: {
            enable: true,
            idKey: "id",
            pIdKey: "pid",
            rootPId: "root"
        }
    },
    callback: {
        onClick: onClick
    }
};

function Node(id, pid, name, isParent) {
    this.id = id;
    this.pid = pid;
    this.name = name;
    this.isParent = isParent;
};
function getUrl(treeId, treeNode) {
    return "api/v2/zookeeper/node?path=" + treeNode.id+"&clusterName="+$("#clusterName").val();
}

function filter(treeId, parentNode, responseData) {
    if (!responseData)
        return null;
    var childNodes = [];
    var nodes = responseData.nodes;
    var length = nodes.length;
    for (var i = 0; i < length; i++) {
        var childNode = nodes[i];
        childNode.pid = parentNode.id;
        childNode.isParent = childNode.parent;
        childNodes.push(childNode);
    }
    return childNodes;
}

//GET zookeeper node data
function getData(path) {
    var clusterName = $("#clusterName").val();
    $.ajax({
        url: "api/v2/zookeeper/node/data",
        type: "get",
        dataType: "json",
        contentType: "application/json;charset=utf-8",
        data: {
            path: path,
            clusterName:clusterName
        },
        success: function (data) {
            $("#data").val(data.data);
        },
        error: function (data) {
           alert("Get zookeeper data error!")
        }
    });
}

function onClick(event, treeId, treeNode, clickFlag) {
    getData(treeNode.id);
}

// show cluster root
function showZKRoot() {
    var treeNodes = [];
    $.ajax({
        url: "api/v2/zookeeper/node",
        type: "get",
        dataType: "json",
        contentType: "application/json;charset=utf-8",
        data: {
            path: "/",
            clusterName: $("#clusterName").val()
        },
        success: function (data) {
            var nodes = data.nodes;
            if (!nodes){
                $("#zkTree").html("Get zookeeper data failure！");
                return;
            }

            for (var i = 0; i < nodes.length; i++) {
                var item = nodes[i];
                treeNodes.push(new Node(item.id, item.pid, item.name,
                    item.parent));
            }
            if (nodes.length < 1)
                $("#zkTree").html("Get zookeeper data failure！");
            else {
                $.fn.zTree.init($("#zkTree"), setting, treeNodes);
                zTree = $.fn.zTree.getZTreeObj("zkTree");
                rMenu = $("#rMenu");
            }
        },
        error: function (data) {
            $("#zkTree").html("Get zookeeper data failure！");
        }
    });
}

var zTree, rMenu;
$(document).ready(function () {
    rMenu = $("#rMenu");
});