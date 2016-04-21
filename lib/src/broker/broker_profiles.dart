part of dsbroker.broker;

Map brokerProfileMap = {
  "broker": {
    "userNode": {
      "addChild": {
        r"$invokable": "config",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
//      "addLink": {
//        r"$invokable": "config",
//        r"$params": [
//          {"name": "Name", "type": "string"},
//          {"name": "Id", "type": "string"}
//        ]
//      },
      "removeNode": {r"$invokable": "config",
        r"$params": [
         {"name": "Recursive", "type": "bool"}
        ]
      }
    },
    "userRoot": {
      "addChild": {
        r"$invokable": "config",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "addLink": {
        r"$invokable": "config",
        r"$params": [
          {"name": "Name", "type": "string"},
          {"name": "Id", "type": "string"}
        ]
      }
    },
    "dataNode": {
      "addNode": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "addValue": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"},
          {
            "name": "Type",
            "type": "enum[string,number,bool,array,map,binary,dynamic]"
          },
          {
            "name": "Editor",
            "type": "enum[none,textarea,password,daterange,date]"
          }
        ]
      },
      "deleteNode": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Recursive", "type": "bool"}
        ]
      },
      "renameNode": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "duplicateNode": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
    },
    "dataRoot": {
      "addNode": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "addValue": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"},
          {
            "name": "Type",
            "type": "enum[string,number,bool,array,map,binary,dynamic]"
          },
          {
            "name": "Editor",
            "type": "enum[none,textarea,password,daterange,date]"
          }
        ]
      },
      "publish": {
        r"$invokable": "write",
        r"$params": [
          {"name": "Path", "type": "string"},
          {"name": "Value", "type": "dynamic"},
          {"name": "Timestamp", "type": "string"},
          {"name": "CloseStream", "type": "bool"}
        ]
      }
    },
    "token": {
      "delete": {r"$invokable": "config", r"$params": []}
    },
    "tokenGroup": {
      "add": {
        r"$invokable": "config",
        r"$params": [
          {"name": "TimeRange", "type": "string", "editor": "daterange"},
          {
            "name": "Count",
            "type": "number",
            "description": "how many times this token can be used"
          },
          {
            "name": "Managed",
            "type": "bool",
            "description":
                "when a managed token is deleted, server will delete"
                  " all the dslinks associated with the token"
          },
          {"name": "Group", "type": "string", "description": "default permission group"},
        ],
        r"$columns": [
          {"name": "tokenName", "type": "string"}
        ]
      }
    }
  }
};
