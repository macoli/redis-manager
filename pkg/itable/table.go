package itable

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/alexeyco/simpletable"
)

//ShowTable 通过表格展示数据
func ShowTable(headerCells []*simpletable.Cell, bodyCells [][]*simpletable.Cell) {
	// 新建表格
	table := simpletable.New()
	// 设置表头
	table.Header = &simpletable.Header{
		Cells: headerCells,
	}
	// 填充表数据
	table.Body.Cells = bodyCells
	// 设置表格样式
	table.SetStyle(simpletable.StyleUnicode)
	// 打印表格
	fmt.Println(table.String())
}

//GetHeaderCells 生成表头
func GenHeaderCells(m interface{}) []*simpletable.Cell {
	t := reflect.TypeOf(m)
	Cells := []*simpletable.Cell{
		{Align: simpletable.AlignRight, Text: "ID"},
	}
	for i := 0; i < t.NumField(); i++ {
		cell := simpletable.Cell{Align: simpletable.AlignCenter, Text: t.Field(i).Name}
		Cells = append(Cells, &cell)
	}
	return Cells
}

//GetBodyCells 生成表数据
func GenBodyCells(m []interface{}) [][]*simpletable.Cell {
	var Cells [][]*simpletable.Cell
	cnt := 1
	for _, row := range m {
		var Cell = []*simpletable.Cell{
			{Align: simpletable.AlignLeft, Text: strconv.Itoa(cnt)},
		}
		for _, item := range row.([]string) {
			c := simpletable.Cell{Align: simpletable.AlignLeft, Text: item}
			Cell = append(Cell, &c)
		}
		Cells = append(Cells, Cell)
		cnt += 1
	}
	return Cells
}
