part of dsbroker.broker;

Map<String, dynamic> brokerProfileMap = {
  "broker": {
    "userNode": {
      "addChild": {
        r"$name": "Add Child",
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
      "removeNode": {
        r"$name": "Remove",
        r"$invokable": "config",
        r"$params": [
         {"name": "Recursive", "type": "bool"}
        ]
      }
    },
    "userRoot": {
      "addChild": {
        r"$name": "Add Child",
        r"$invokable": "config",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "addLink": {
        r"$name": "Add Link",
        r"$invokable": "config",
        r"$params": [
          {"name": "Name", "type": "string"},
          {"name": "Id", "type": "string"}
        ]
      }
    },
    "dataNode": {
      "addNode": {
        r"$name": "Add Node",
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ],
        r"$actionGroup": "Add",
        r"$actionGroupSubTitle": "Node"
      },
      "addValue": {
        r"$name": "Add Value",
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
        ],
        r"$actionGroup": "Add",
        r"$actionGroupSubTitle": "Value"
      },
      "deleteNode": {
        r"$name": "Delete Node",
        r"$invokable": "write",
        r"$params": [
          {"name": "Recursive", "type": "bool"}
        ]
      },
      "renameNode": {
        r"$name": "Rename Node",
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "duplicateNode": {
        r"$name": "Duplicate Node",
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ]
      },
      "exportNode": {
        r"$name": "Export",
        r"$invokable": "write",
        r"$columns": [
          {
            "name": "data",
            "type": "string",
            "editor": "textarea"
          }
        ]
      },
      "importNode": {
        r"$name": "Import",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "data",
            "type": "string",
            "editor": "textarea"
          }
        ]
      }
    },
    "dataRoot": {
      "addNode": {
        r"$name": "Add Node",
        r"$invokable": "write",
        r"$params": [
          {"name": "Name", "type": "string"}
        ],
        r"$actionGroup": "Add",
        r"$actionGroupSubTitle": "Node"
      },
      "addValue": {
        r"$name": "Add Value",
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
        ],
        r"$actionGroup": "Add",
        r"$actionGroupSubTitle": "Value"
      },
      "publish": {
        r"$name": "Publish",
        r"$invokable": "write",
        r"$params": [
          {"name": "Path", "type": "string"},
          {"name": "Value", "type": "dynamic"},
          {"name": "Timestamp", "type": "string"},
          {"name": "CloseStream", "type": "bool"}
        ]
      },
      "exportNode": {
        r"$name": "Export",
        r"$invokable": "write",
        r"$columns": [
          {
            "name": "data",
            "type": "string",
            "editor": "textarea"
          }
        ]
      },
      "importNode": {
        r"$name": "Import",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "data",
            "type": "string",
            "editor": "textarea"
          }
        ]
      }
    },
    "token": {
      "delete": {
        r"$name": "Delete",
        r"$invokable": "config",
        r"$params": []
      },
      "update": {
        r'$name': 'Update Group',
        r'$invokable': 'config',
        r'$params': [
          {
            'name': 'Group',
            'type': 'string',
            'description': 'default permission group'
          }
        ]
      }
    },
    "tokenGroup": {
      "add": {
        r"$name": "Add Token",
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
          {
            "name": "Group",
            "type": "string",
            "description": "default permission group"
          },
        ],
        r"$columns": [
          {"name": "tokenName", "type": "string"}
        ]
      }
    }
  }
};
