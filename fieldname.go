package sqlchemy

import (
	"reflect"

	"yunion.io/x/pkg/util/reflectutils"
)

func GetFieldStruct(structPtr interface{}, fieldPtr interface{}) *reflect.StructField {
	structValue := reflect.Indirect(reflect.ValueOf(structPtr))
	structUintptr := reflect.ValueOf(structPtr).Pointer()
	fieldUintptr := reflect.ValueOf(fieldPtr).Pointer()
	structType := structValue.Type()
	offset := fieldUintptr - structUintptr
	return getFieldStruct(structType, offset)
}

func getFieldStruct(structType reflect.Type, offset uintptr) *reflect.StructField {
	for i := 0; i < structType.NumField(); i += 1 {
		fieldType := structType.Field(i)
		if fieldType.Offset > offset {
			break
		}
		if fieldType.Anonymous {
			return getFieldStruct(fieldType.Type, offset-fieldType.Offset)
		} else {
			if fieldType.Offset == offset {
				return &fieldType
			}
		}
	}
	return nil
}

func GetColumnName(structPtr interface{}, fieldPtr interface{}) string {
	sf := GetFieldStruct(structPtr, fieldPtr)
	if sf == nil {
		return ""
	}
	info := reflectutils.ParseStructFieldJsonInfo(*sf)
	return info.MarshalName()
}

var (
	fieldAddrName map[uintptr]string
)

func init() {
	fieldAddrName = make(map[uintptr]string)
}

func RegisterStructFieldNames(structPtr interface{}) {
	structValue := reflect.ValueOf(structPtr).Elem()
	structUintptr := reflect.ValueOf(structPtr).Pointer()
	structType := structValue.Type()
	registerStructFieldNames(structType, structUintptr)
}

func registerStructFieldNames(structType reflect.Type, startAddr uintptr) {
	for i := 0; i < structType.NumField(); i += 1 {
		fieldType := structType.Field(i)
		if fieldType.Anonymous {
			registerStructFieldNames(fieldType.Type, startAddr+fieldType.Offset)
		} else {
			info := reflectutils.ParseStructFieldJsonInfo(fieldType)
			fieldAddrName[startAddr+fieldType.Offset] = info.MarshalName()
		}
	}
}

func Fn(field interface{}) string {
	fieldUintptr := reflect.ValueOf(field).Pointer()
	if name, ok := fieldAddrName[fieldUintptr]; ok {
		return name
	}
	return ""
}
