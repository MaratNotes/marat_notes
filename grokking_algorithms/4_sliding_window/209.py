from typing import List

class Solution:
    def minSubArrayLen(self, target: int, nums: List[int]) -> int:        
        left = 0
        window_sum = 0
        min_len = float('inf')
        
        for right in range(len(nums)):
            window_sum += nums[right]               # расширяем окно
            while window_sum >= target:             # пока условие выполнено — сжимаем
                min_len = min(min_len, right - left + 1)
                window_sum -= nums[left]
                left += 1

        if min_len == float('inf'): # приведем ответ к условию задачи
            min_len = 0

        return min_len
        return min_len