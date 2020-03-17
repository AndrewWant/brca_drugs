import math as mt
from numpy import zeros

#-------------------------------------------------------------
def num2comma(num):
	
	if mt.isinf(num):
	    return "Infinity"
	elif mt.isinf(-num):
		return "-Infinity"
	elif num==0:
		return "0"
	
	order = mt.log10(num)
	if (order/3) > int(order/3):
		groups = int(order/3+1)
	else:
		groups = int(order/3)
	
	x = zeros([groups,1])
	prev = num
	s = ""
	for i in range(groups):
		x[i] = int(prev/10**(3*(groups-i-1)))
		prev -= int(x[i]*10**(3*(groups-i-1)))
		if i < groups-1:
			s+=str('%03d' % int(x[i]))+","
		else:
			s+=str('%03d' % int(x[i]))
	
	return s
#----------------------------------------------------------

# -----------------------------------------------
def back2num(code):
    
	return fore2num(code) + 10


# ------------------------------------------------
def fore2num2(code, bright):
    
	if bright == 0:
		return fore2num(code)
	else:
		return fore2num(code) + 60

# ---------------------------------------------

def fore2num(code):
    """Conversion of letter code for colour to numerical value.
    """
	code_dict = {"k": 30, 
				 "r": 31, 
				 "g": 32, 
				 "y": 33, 
				 "b": 34, 
				 "m": 35, 
				 "c": 36, 
				 "w": 37
				}
	try:
		return code_dict[code]
	except KeyError:
		print 'Illegal code in *fore2num* !!!'
		return 0

# ------------------------------------------------
def back2num2(code, bright):
    
	if bright == 0:
		return back2num(code)
	else:
		return back2num(code) + 6

# -------------------------------------------------
def rich_string_wrap(
	s,
    codef,
    bf,
    codeb,
    bb,
    ):
	return "\033[1;" + str(fore2num2(codef, bf)) + ';' \
        + str(back2num2(codeb, bb)) + 'm' + s + "\033[0m"

# ---------------------------------------------------------
#============================================================
""" Convert from polar (r,w) to rectangular (x,y) x = r cos(w) y = r sin(w) """ 
def rect(r, w, deg=0): 
# radian if deg=0; degree if deg=1 
    from math import cos, sin, pi 
    if deg: 
        w = pi * w / 180.0 
        return r * cos(w), r * sin(w)

""" Convert from rectangular (x,y) to polar (r,w) r = sqrt(x^2 + y^2) w = arctan(y/x) = [-\pi,\pi] = [-180,180] """ 
def polar(x, y, deg=0): 
    # radian if deg=0; degree if deg=1 
    from math import hypot, atan2, pi 
    if deg: 
        return hypot(x, y), 180.0 * atan2(y, x) / pi 
    else: 
        return hypot(x, y), atan2(y, x) 
