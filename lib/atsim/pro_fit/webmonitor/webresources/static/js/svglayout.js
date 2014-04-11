var _getHandleCoord = function(handle, container )
{
	var vertKey = /([nms])/.exec(handle)[1];
	var horizKey = /([ecw])/.exec(handle)[1];
	var key = vertKey + horizKey;
	var bbox = container[0][0].getBBox();
	var coord = {};
	var ele = $(container[0][0]);
	var p = ele.position();

	var x = { w: p.left,
			  c: (bbox.width / 2.0) + p.left,
			  e: p.left + bbox.width
			}[horizKey];

	var y = { n: p.top,  
			  m: (bbox.height / 2.0) + p.top,
			  s: p.top + bbox.height
			}[vertKey];
	
	return { 'x': x, 'y': y };	
};

var _extents = function(container)
{
	var extents = {}
	$(container[0][0]).each(function(i,ele){
		var p = $(ele).position();
		var bbox = ele.getBBox();
		var l = p.left;
		var r = l+bbox.width;
		var t = p.top;
		var b = t + bbox.height;

		extents.l = extents.l == null || l < extents.l ? l : extents.l;
		extents.r = extents.r == null || r > extents.r ? r : extents.r;
		extents.t = extents.t == null || t < extents.t ? t : extents.t;
		extents.b = extents.b == null || b > extents.b ? b : extents.b;
	});
	return extents;
};

var snapTo = function(handle1, container1, handle2, container2)
/** Move a particular control handle of container1 so it is located over 
  * handle of container2.
  *
  * Handles are defined as two letter strings with one letter
  * representing vertical position whilst the other giving horizontal
  * position. 
  *
  * Vertical key can be one of:
  *   n : north
  *   m : middle
  *   s : south
  *
  * Horizontal keys are:
  *   w : west
  *   c : centre
  *   e : east
  *
  * @param handle1 Control handle for container1 (see above for definition)
  * @param container1 d3 selection giving svg element to be aligned
  * @param handle2 Control handle of container2 against which alignment should take place
  * @param container2 d3 selection giving svg element that container1 will be aligned against
  */
{
	var handle1Coord = _getHandleCoord(handle1, container1);
	var handle2Coord = _getHandleCoord(handle2, container2);

	//Move handle1 over handle 2
	var moveVec = { x: handle2Coord.x - handle1Coord.x, y: handle2Coord.y - handle1Coord.y};
	var transform = d3.transform(container1.attr('transform'));
	transform.translate[0] += moveVec.x;
	transform.translate[1] += moveVec.y;
	container1.attr('transform', transform);
};



var horizontalAlign = function(key, container)
/** Align children of container horizontally
  * 
  * @param key One of l, c or r, representing left, centre or right respectively.
  * @param container d3 selection containing items to be aligned.
  */
{
	var extents = _extents(container);
	var v = { 
		'l' : function(container){
			$(container[0][0]).children().each(function(i, ele){
				var p = $(ele).position();
				var localselect = d3.select(ele);
				var trans = d3.transform(localselect.attr('transform'));
				trans.translate[0] = trans.translate[0] + (extents.l - p.left);
				localselect.attr('transform', trans);
			});
		},

		'c' : function(container){
			var c = extents.l + (extents.r - extents.l)/2.0;
			var lc;
			$(container[0][0]).children().each(function(i, ele){
				var p = $(ele).position();
				var bbox = ele.getBBox();
				var lr = p.left + bbox.width;
				localselect = d3.select(ele);
				trans = d3.transform(localselect.attr('transform'));
				trans.translate[0] = trans.translate[0] + (c-p.left) - bbox.width/2.0;
				localselect.attr('transform', trans);
			});
		},

		'r' : function(container){
			var localselect;
			$(container[0][0]).children().each(function(i, ele){
				var bbox = ele.getBBox();
				var lr = $(ele).position().left + bbox.width;
				localselect = d3.select(ele);
				trans = d3.transform(localselect.attr('transform'));
				trans.translate[0] = trans.translate[0] + (extents.r - lr);
				localselect.attr('transform', trans);
			});
		}
	};
	v[key](container);
};


var verticalAlign = function(key, container)
/** Align children of container vertically
  * 
  * @param key One of n, m, s for north (top), middle or south (bottom).
  * @param container d3 selection containing items to be aligned.
  */
 {
 	var extents = _extents(container);
 	var v = {
 		n : function(container){
 			//Find the lowest y coord and then translate all items to have same y coord.
 			$(container[0][0]).children().each(function(i,ele){
 				var localselect = d3.select(ele);
 				var trans = d3.transform(localselect.attr('transform'));
 				trans.translate[1] = trans.translate[1] + (extents.t - $(ele).position().top);
 				localselect.attr('transform', trans);
 			});
 		},

 		m : function(container){
 			//Translate each child to have the same centre
 			var c = extents.t + (extents.b - extents.t)/2.0;
 			$(container[0][0]).children().each(function(i,ele){
 				var localselect = d3.select(ele);
 				var trans = d3.transform(localselect.attr('transform'));
 				trans.translate[1] = trans.translate[1] + (c - $(ele).position().top) - ele.getBBox().height/2.0;
 				localselect.attr('transform', trans);
 			});
 		},

 		s : function(container){
 			//Find the highest y coord and then translate all items to have same y coord.
 			$(container[0][0]).children().each(function(i,ele){
 				var localselect = d3.select(ele);
 				var trans = d3.transform(localselect.attr('transform'));
 				trans.translate[1] = trans.translate[1] + (extents.b - ($(ele).position().top + ele.getBBox().height));
 				localselect.attr('transform', trans);
 			});
 		}
 	};
 	v[key](container);
 };


var topToBottomLayout = function(container, gap)
{
	var y = 0;
	var width = 0;
	var bbox;

	$(container[0][0]).children().each(function(i, ele){
		var localselect = d3.select(ele);

		// Work out transform to move to (0,0)
		var tx, ty;
		localselect.attr('transform', 'translate(0,0)');
		bbox = this.getBBox();

		tx = -bbox.x;
		ty = - bbox.y;

		if (bbox.width > width) {
			width = bbox.width;
		}
		var trans = d3.transform(localselect.attr('transform'));
		trans.translate[0] = tx;
		trans.translate[1] = y+ty;
		y += bbox.height + gap;
		localselect.attr('transform', trans);
	});

	var height = y + bbox.height;
	container
		.attr('width', width)
		.attr('height', height);

};

var horizontalLayout = function(container, gap)
/** Layout child items of container from left to right with gap between them.
 *  @param container Element containing children to layout.
 *  @param gap Gap between elements
 */
{
	var x = 0;
	var width = 0;
	var height = 0;
	var bbox;

	$(container[0][0]).children().each(function(i,ele){
		var localselect = d3.select(ele);
		// Work out transform to move to (0,0)
		var tx, ty;
		localselect.attr('transform', 'translate(0,0)');
		bbox = this.getBBox();

		if (bbox.height > height)
		{
			height = bbox.height;
		}

		tx = -bbox.x;
		ty = - bbox.y;

		var trans = d3.transform(localselect.attr('transform'));
		trans.translate[0] = tx + x;
		trans.translate[1] = ty;
		localselect.attr('transform', trans);

		x+= bbox.width + gap;
	});

	container
		.attr('width', x - gap)
		.attr('height', height);
};
