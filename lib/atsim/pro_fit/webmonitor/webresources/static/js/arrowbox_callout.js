var drawArrowBox = function(selection, x,y, ax, ay, width, height, side, arrowOffset, arrowAngle)
/** 
  * @param selection d3 selection into which box elements will be placed.
  * @param x box x-pos.
  * @param y box y-pos.
  * @param ax attachment position on x-axis.
  * @param ay attachment position on y-axis.
  * @param width Box width.
  * @param height Box height.
  * @param side string One of north, south, east or west giving side that arrow sits on.
  * @param arrowOffset Distance of arrow tip from side.
  * @param arrowAngle Angle arrow edges make to side in degrees.
  */
{
	var arrowAngleRad = (arrowAngle/360.0) * 2.0 * Math.PI;
	var arrowSideLengthOffset = arrowOffset / Math.tan(arrowAngleRad);

	var margin = 2;

	var calcAttachPoint = function(ax, ay, sx, sy, ex, ey)
	{
		var sidevec = {x : ex - sx, y : ey - sy};
		var avec = {x : ax - sx, y : ay - sy};
		var mag = sidevec.x * sidevec.x + sidevec.y * sidevec.y;
		mag = Math.sqrt(mag);
		sidevec.x = sidevec.x / mag;
		sidevec.y = sidevec.y / mag;
		var normvec = { x : sidevec.y, y: -sidevec.x};
		var sideoffsetFrac = (arrowSideLengthOffset / mag) + margin/mag;

		// Take dot product to map avec onto sidevec
		var fracpos =	(sidevec.x * avec.x + sidevec.y * avec.y)/mag;
		if (fracpos < sideoffsetFrac) 
		{
			fracpos = sideoffsetFrac;
		}
		else if (fracpos > 1.0 - sideoffsetFrac)
		{
			fracpos = 1.0 - sideoffsetFrac
		}

		var arrowDetach, arrowTip, arrowAttach;
		var arrowOffsetVec = { x: sidevec.x * arrowSideLengthOffset, y : sidevec.y * arrowSideLengthOffset};

		arrowTip = { x : sx + sidevec.x * fracpos * mag, y : sy + fracpos * sidevec.y * mag};
		arrowDetach = { x : arrowTip.x - arrowOffsetVec.x, y : arrowTip.y - arrowOffsetVec.y };
		arrowAttach = { x : arrowTip.x + arrowOffsetVec.x, y : arrowTip.y + arrowOffsetVec.y };

		//Offset arrowTip along normal by the arrow offset
		arrowTip.x = arrowTip.x + normvec.x * arrowOffset;
		arrowTip.y = arrowTip.y + normvec.y * arrowOffset;
		return [arrowDetach, arrowTip, arrowAttach];
	};

	var boxPoints = function(b)
	{
		return [ {x : b.x, y: b.y}, 
				 {x : b.x+b.width, y: b.y}, 
				 {x : b.x+b.width, y : b.y+b.height}, 
				 {x : b.x, y: b.y+b.height}];
	};

	var splicePoints = function(points, insert, cutIdx)
	{
		var rightPoints = points.splice(cutIdx);
		return $.merge($.merge(points, insert), rightPoints);
	};

	var innerBox = {};
	var attachPos;
	var points = [];
	var v = { 
		'north' : function() {innerBox.x = x; innerBox.y = y + arrowOffset; innerBox.width=width; innerBox.height=height - arrowOffset; 
			attachPos = calcAttachPoint(ax,ay, innerBox.x, innerBox.y, innerBox.x+innerBox.width, innerBox.y);
			points = splicePoints(boxPoints(innerBox), attachPos, 1);
		},
		'south' : function() {innerBox.x = x; innerBox.y = y; innerBox.width=width; innerBox.height=height - arrowOffset;
			attachPos = calcAttachPoint(ax,ay, innerBox.x+innerBox.width, innerBox.y+innerBox.height, innerBox.x, innerBox.y+innerBox.height);
			points = splicePoints(boxPoints(innerBox), attachPos, 3);
		} ,
		'west' 	: function() {innerBox.x = x+arrowOffset; innerBox.y = y; innerBox.width=width - arrowOffset; innerBox.height=height; 
			attachPos = calcAttachPoint(ax,ay, innerBox.x, innerBox.y+innerBox.height, innerBox.x, innerBox.y);
			points = splicePoints(boxPoints(innerBox), attachPos, 4);
		} ,
		'east'	: function() {innerBox.x = x; innerBox.y = y; innerBox.width=width-arrowOffset; innerBox.height=height;
			attachPos = calcAttachPoint(ax,ay, innerBox.x+innerBox.width, innerBox.y, innerBox.x+innerBox.width, innerBox.y+innerBox.height);
			points = splicePoints(boxPoints(innerBox), attachPos, 2);
		} 
	}[side]();

	// Join points into a d attr string
	var cmd = "M";
	var d = ""
	$.each(points, function(i,v){
		d += cmd+' '+v.x+' '+v.y+' ';
		cmd = 'L';
	});
	d += ' z';
	
	selection.append('path')
		.attr('d', d);

	return innerBox;
};

var drawCallout = function(selection, x,y, offset, contentCallable)
{
	// Create some content

	var callout = selection.append('g');
	callout.attr('class', 'callout');

	var bg = callout.append('g');
	var content = callout.append('g');

	contentCallable(content);	

	var padding = 3;

	var width = content[0][0].getBBox().width + 2*padding;
	var height = content[0][0].getBBox().height + 2*padding;
	height += 8;

	var side;
	var bbox = selection[0][0].getBBox();
	var rx,ry;
	rx = x-(width/2.0);

	if (rx < 0)
	{
		rx = 5;
	}
	else if ( rx + width > bbox.width) 
	{
		rx = bbox.width - width - 5;
	}

	if (y > (bbox.height - y))
	{
		// y coord is further from north side
		ry = y - height - offset;
		side = 'south';
	}
	else
	{
		ry = y + offset;
		side = 'north';
	}

	var ib = drawArrowBox(
		bg,
		rx, ry,
		x, y,
		width,
		height,
		side,
		8,
		42);

	content.attr('transform', 'translate('+(ib.x+padding)+','+(ib.y+padding)+')');
};