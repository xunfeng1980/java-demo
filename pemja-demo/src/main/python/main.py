from pemja import findClass

StringBuilder = findClass('java.lang.StringBuilder')
Integer = findClass('java.lang.Integer')

def callback_java():
    sb = StringBuilder()
    sb.append('pemja')
    sb.append('java')
    sb.append('python')
    sb.append(Integer.toHexString(Integer.MAX_VALUE))
    return sb.toString()

print(callback_java())


