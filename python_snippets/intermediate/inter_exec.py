# Description
# Given a list of intervals represented as tuples (start, end), merge all overlapping intervals and return a list of non-overlapping intervals.
# Example:

# Input: [(1, 3), (2, 6), (8, 10), (15, 18)]
# Output: [(1, 6), (8, 10), (15, 18)]

# Constraints:

# Intervals are not necessarily sorted
# Each interval has start <= end
# Return intervals in sorted order

def merge_intervals(intervals):
    if not intervals:
        return []
    
    # Sort intervals by start time
    intervals.sort(key=lambda x: x[0])
    
    merged = [intervals[0]]
    
    for current in intervals[1:]:
        last_merged = merged[-1]
        
        # If current interval overlaps with the last merged interval
        if current[0] <= last_merged[1]:
            # Merge by updating the end time
            merged[-1] = (last_merged[0], max(last_merged[1], current[1]))
        else:
            # No overlap, add current interval
            merged.append(current)
    
    return merged


# Description
# Given the root of a binary tree, return a list of lists where each inner list contains the values of nodes at that level, from left to right.
# Example:
#     3
#    / \
#   9   20
#      /  \
#     15   7

# Output: [[3], [9, 20], [15, 7]]


from collections import deque

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def level_order_traversal(root):
    if not root:
        return []
    
    result = []
    queue = deque([root])
    
    while queue:
        level_size = len(queue)
        current_level = []
        
        # Process all nodes at current level
        for _ in range(level_size):
            node = queue.popleft()
            current_level.append(node.val)
            
            # Add children to queue for next level
            if node.left:
                queue.append(node.left)
            if node.right:
                queue.append(node.right)
        
        result.append(current_level)
    
    return result


# Description
# Given an integer array nums, return an array answer such that answer[i] is equal to the product of all elements in nums except nums[i].
# Constraints:

# You must solve it without using division
# The algorithm should run in O(n) time
# Use only O(1) extra space (not counting the output array)

# Example:

# Input: [1, 2, 3, 4]
# Output: [24, 12, 8, 6]


def product_except_self(nums):
    n = len(nums)
    result = [1] * n
    
    # First pass: calculate left products
    # result[i] will contain product of all elements to the left of i
    for i in range(1, n):
        result[i] = result[i-1] * nums[i-1]
    
    # Second pass: multiply with right products
    # right_product keeps track of product of elements to the right
    right_product = 1
    for i in range(n-1, -1, -1):
        result[i] *= right_product
        right_product *= nums[i]
    
    return result

# Description
# Given a string containing only parentheses characters '(', ')', '{', '}', '[', ']', determine if the input string has valid parentheses. The string is valid if:

# Open brackets are closed by the same type of brackets
# Open brackets are closed in the correct order
# Every close bracket has a corresponding open bracket

# Example:

# Input: "()[]{}"  → Output: True
# Input: "([)]"    → Output: False
# Input: "{[]}"    → Output: True


def is_valid_parentheses(s):
    stack = []
    mapping = {')': '(', '}': '{', ']': '['}
    
    for char in s:
        if char in mapping:  # Closing bracket
            if not stack or stack.pop() != mapping[char]:
                return False
        else:  # Opening bracket
            stack.append(char)
    
    return len(stack) == 0

# Description
# Given a string s, find the length of the longest substring without repeating characters.
# Example:

# Input: "abcabcbb"  → Output: 3 (substring "abc")
# Input: "bbbbb"     → Output: 1 (substring "b")
# Input: "pwwkew"    → Output: 3 (substring "wke")

# Constraints:

# 0 <= s.length <= 50000
# s consists of English letters, digits, symbols and spaces


def length_of_longest_substring(s):
    if not s:
        return 0
    
    char_index = {}  # Maps character to its most recent index
    left = 0  # Left pointer of sliding window
    max_length = 0
    
    for right in range(len(s)):
        char = s[right]
        
        # If character is repeated and within current window
        if char in char_index and char_index[char] >= left:
            left = char_index[char] + 1
        
        char_index[char] = right
        max_length = max(max_length, right - left + 1)
    
    return max_length

# Given an array of strings strs, group the anagrams together. You can return the answer in any order.
# An anagram is a word formed by rearranging the letters of another word, using all original letters exactly once.
# Example:

# Input: ["eat","tea","tan","ate","nat","bat"]
# Output: [["bat"],["nat","tan"],["ate","eat","tea"]]

# Constraints:

# 1 <= strs.length <= 10^4
# 0 <= strs[i].length <= 100
# strs[i] consists of lowercase English letters only

from collections import defaultdict

def group_anagrams(strs):
    anagram_groups = defaultdict(list)
    
    for s in strs:
        # Sort characters to create a key for anagrams
        # All anagrams will have the same sorted key
        key = ''.join(sorted(s))
        anagram_groups[key].append(s)
    
    return list(anagram_groups.values())

# Alternative solution using character frequency as key
def group_anagrams_v2(strs):
    anagram_groups = defaultdict(list)
    
    for s in strs:
        # Create frequency count as tuple (immutable, can be dict key)
        char_count = [0] * 26
        for char in s:
            char_count[ord(char) - ord('a')] += 1
        
        key = tuple(char_count)
        anagram_groups[key].append(s)
    
    return list(anagram_groups.values())
