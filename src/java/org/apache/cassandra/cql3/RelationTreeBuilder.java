package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Stack;

import org.apache.cassandra.cql3.Relation.LogicalOperator;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class used used by CQL3 ANTLR grammar to build a relation tree based on the WHERE clause of the select statement,
 * couple of examples of how those trees are going to look like and steps used to build them are as follows:
 *
 * first_name = 'x' AND last_name = 'y' OR age > 5 AND age < 7
 *                      AND
 *                     /  \
 *                    OR   age < 7
 *                   /  \
 *                 AND   age > 5
 *                /   \
 *    first_name:x     last_name:y
 *
 *       # <first_name:x> // add first_name:x
 *       # <logical-AND left:<first_name:x> right:null> // add logical AND
 *       # <logical-AND left:<first_name:x> right:<last_name:y>> // add last_name:y
 *       # <logical-OR  left:<logical-AND left:<first_name:x> right:<last_name:y>> right:null> // add logical OR
 *       # <logical-OR  left:<logical-AND left:<first_name:x> right:<last_name:y>> right:age ">" 5> // add age > 5
 *       # <logical-AND left:<logical-OR  left:<logical-AND left:<first_name:x> right:<last_name:y>> right:age ">" 5> right: null> // add logical AND
 *       # <logical-AND left:<logical-OR  left:<logical-AND left:<first_name:x> right:<last_name:y>> right:age ">" 5> right: age "<" 7> // add age < 7
 *
 * first_name = 'x' AND (first_name = 'y' OR last_name = 'z') OR age > 5
 *                       OR
 *                      /  \
 *                   AND    age > 5
 *                  /   \
 *      first_name:x     OR
 *                      /  \
 *          first_name:y    last_name:z
 *
 *       #1 <first_name:x> // add first_name:x
 *       #2 <logical-AND left:<first_name:x> right:null> // add logical AND
 *       #3 set depth to 1 // open parenthesis
 *       #4 <logical-AND left:<first_name:x> right:<first_name:y>> // add first_name:y
 *       #5 <logical-AND left:<first_name:x> right:<logical-OR left:first_name:y right:null>> // add logical OR
 *       #6 <logical-AND left:<first_name:x> right:<logical-OR left:first_name:y right:last_name:z>> // add last_name:z
 *       #7 set depth to 0 // close parenthesis
 *       #8 <logical-OR left:<logical-AND left:<first_name:x> right:<logical-OR left:first_name:y right:last_name:z>> right: null> // add logical OR
 *       #9 <logical-OR left:<logical-AND left:<first_name:x> right:<logical-OR left:first_name:y right:last_name:z>> right: age ">" 5> // add age > 5
 */
public class RelationTreeBuilder
{
    private final Stack<Relation> relationStack = new Stack<>();
    private Relation root;

    public void add(LogicalRelation logical)
    {
        if (relationStack.empty())
        {
            logical.setLeftRelation(root);
            root = logical;
        }
        else
        {
            Relation current = relationStack.peek();
            if (current.logicalOperator == null)
            {
                current.logicalOperator = logical.logicalOperator;
            }
            else
            {

                logical.setLeftRelation(current.getRightRelation());
                current.setRightRelation(logical);
                relationStack.push(logical);
            }
        }
    }

    public void add(List<Relation> relation)
    {
        if (root == null)
        {
            root = flatten(relation);
            return;
        }

        addColumnRelation(flatten(relation));
    }

    public void incrementDepth()
    {
        Relation groupOp = new LogicalRelation();

        if (relationStack.empty())
        {
            root.setRightRelation(groupOp);
        }
        else
        {
            Relation current = relationStack.peek();
            if (current.getLeftRelation() == null)
                current.setLeftRelation(groupOp);
            else
                current.setRightRelation(groupOp);
        }

        relationStack.push(groupOp);
    }

    public void decrementDepth()
    {
        while (!relationStack.empty())
        {
            Relation current = relationStack.peek();
            if (current.logicalOperator == null)
                break;

            relationStack.pop();
        }
    }

    public Relation build()
    {
        return root;
    }

    private void addColumnRelation(Relation column)
    {
        if (relationStack.empty())
        {
            root.setRightRelation(column);
            return;
        }

        Relation current = relationStack.peek();
        if (current.getLeftRelation() == null)
            current.setLeftRelation(column);
        else if (current.getRightRelation() == null)
            current.setRightRelation(column);
    }

    /**
     * Flattens list of the relations into a tree using AND logical relations.
     * e.g.  [1, 2, 3] is going to become:
     *          AND
     *         /   \
     *       AND    3
     *      /   \
     *     1     2
     * @param relations The list of relations to fallen.
     *
     * @return if list only has a single element only that element is going to be returned,
     *         otherwise flattened tree would be returned.
     */
    @VisibleForTesting
    protected static Relation flatten(List<Relation> relations)
    {
        if (relations.size() == 1)
            return relations.get(0);

        Relation root = new LogicalRelation(LogicalOperator.AND);
        for (Relation relation : relations)
        {
            if (root.getLeftRelation() == null)
            {
                root.setLeftRelation(relation);
            }
            else
            {
                root.setRightRelation(relation);
                Relation newRoot = new LogicalRelation(LogicalOperator.AND);

                newRoot.setLeftRelation(root);
                root = newRoot;
            }
        }

        return root;
    }
}
