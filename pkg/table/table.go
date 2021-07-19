package table

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/alexeyco/simpletable"
)

//ShowTable Show the data by table
func ShowTable(headerCells []*simpletable.Cell, bodyCells [][]*simpletable.Cell) {
	//new a table
	table := simpletable.New()
	//set table header
	table.Header = &simpletable.Header{
		Cells: headerCells,
	}
	//set table body
	table.Body.Cells = bodyCells
	//table style
	table.SetStyle(simpletable.StyleUnicode)
	//print the table
	fmt.Println(table.String())
}

//GetHeaderCells Generate table header data
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

//GetBodyCells Generate table body data
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
