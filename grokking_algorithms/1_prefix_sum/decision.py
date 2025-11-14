# https://leetcode.com/problems/range-sum-query-immutable/
class NumArray:
    def __init__(self, nums):
        """
        Инициализирует объект с целочисленным массивом nums.
        Временная сложность: O(n) для предварительной обработки
        Пространственная сложность: O(n) для хранения префиксных сумм
        """
        # Создаем массив префиксных сумм, начинающийся с 0
        # Это упрощает обработку случаев, когда left = 0
        self.prefix_sums = [0]
        for num in nums:
            # Каждый следующий элемент - это сумма предыдущей префиксной суммы и текущего числа
            self.prefix_sums.append(self.prefix_sums[-1] + num)
    
    def sumRange(self, left, right):
        """
        Возвращает сумму элементов между индексами left и right включительно.
        Временная сложность: O(1) на каждый запрос
        """
        # Сумма от left до right = префиксная сумма в (right + 1) - префиксная сумма в left
        return self.prefix_sums[right + 1] - self.prefix_sums[left]