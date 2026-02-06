from collections import Counter
import heapq

class Solution:
    def topKFrequent(self, nums: List[int], k: int) -> List[int]:
        # Шаг 1: считаем частоты
        freq = Counter(nums)
        
        # Защита от граничных случаев
        if k >= len(freq):
            return list(freq.keys())
        
        # Шаг 2: мин-куча размером k 
        min_heap = []
        for num, count in freq.items():
            if len(min_heap) < k:
                heapq.heappush(min_heap, (count, num))
            elif count > min_heap[0][0]:
                heapq.heapreplace(min_heap, (count, num))
        
        # Шаг 3: извлекаем только элементы
        return [num for _, num in min_heap]