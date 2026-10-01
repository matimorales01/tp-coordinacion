package clientfruit

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type Map map[string]map[string]fruititem.FruitItem

func (fruitMap Map) Add(clientId string, records []fruititem.FruitItem) {
	fruitItemMap, ok := fruitMap[clientId]
	if !ok {
		fruitItemMap = map[string]fruititem.FruitItem{}
		fruitMap[clientId] = fruitItemMap
	}

	for _, record := range records {
		if existing, ok := fruitItemMap[record.Fruit]; ok {
			fruitItemMap[record.Fruit] = existing.Sum(record)
		} else {
			fruitItemMap[record.Fruit] = record
		}
	}
}

func (fruitMap Map) Take(clientId string) []fruititem.FruitItem {
	fruitItemMap := fruitMap[clientId]
	delete(fruitMap, clientId)

	records := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, record := range fruitItemMap {
		records = append(records, record)
	}
	return records
}

func Top(records []fruititem.FruitItem, size int) []fruititem.FruitItem {
	sort.SliceStable(records, func(i, j int) bool {
		return records[j].Less(records[i])
	})
	if size > len(records) {
		size = len(records)
	}
	return records[:size]
}
